import 'dotenv/config';
import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import {
    makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
} from '@whiskeysockets/baileys';
import pino from 'pino';

const PICO_PREFIX = '[🦞:]';
const MAX_STORED_MESSAGES = 500;
const WA_VERSION = [2, 3000, 1033893291];
const WS_OPEN_STATE = 1;
const PICO_PREFIX_REGEX = new RegExp(
    `^${PICO_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*`,
    'i',
);

function normalizePhoneNumber(value) {
    return String(value || '').replace(/[^0-9]/g, '');
}

function extractDigitsFromJid(jid) {
    const userPart = String(jid || '')
        .split('@')[0]
        .split(':')[0];
    return normalizePhoneNumber(userPart);
}

function extractSenderPhone(key) {
    const senderJid = key?.participant || key?.remoteJid || '';
    return extractDigitsFromJid(senderJid);
}

function extractMessageText(message) {
    return message?.conversation || message?.extendedTextMessage?.text || '';
}

function extractOutboundText(payload) {
    if (!payload || typeof payload !== 'object') return '';

    const candidates = [
        payload.content,
        payload.text,
        payload.message?.text,
        payload.message?.content,
    ];

    for (const candidate of candidates) {
        if (typeof candidate === 'string' && candidate.trim()) {
            return candidate.trim();
        }
    }

    return '';
}

function hasPicoPrefix(text) {
    return PICO_PREFIX_REGEX.test(String(text || '').trimStart());
}

function addSinglePicoPrefix(text) {
    const trimmed = String(text || '').trim();
    const withoutPrefix = trimmed.replace(PICO_PREFIX_REGEX, '');
    return withoutPrefix ? `${PICO_PREFIX} ${withoutPrefix}` : PICO_PREFIX;
}

function isLidJid(jid) {
    return String(jid || '').endsWith('@lid');
}

function toWhatsAppUserJid(phoneNumber) {
    const digits = normalizePhoneNumber(phoneNumber);
    return digits ? `${digits}@s.whatsapp.net` : '';
}

function buildTargetCandidates(parsedMessage, waBotPhoneNumber) {
    const candidates = [];
    const seen = new Set();

    const addCandidate = (value) => {
        const candidate = String(value || '').trim();
        if (!candidate || seen.has(candidate)) return;
        seen.add(candidate);
        candidates.push(candidate);
    };

    const chatJid = parsedMessage?.chat;
    const toJid = parsedMessage?.to;
    const jidJid = parsedMessage?.jid;
    const fromJid = parsedMessage?.from;
    const lidCandidate = [chatJid, toJid, jidJid, fromJid].find((jid) =>
        isLidJid(jid),
    );
    const botPnJid = toWhatsAppUserJid(waBotPhoneNumber);

    // Keep strict routing order for same-session behavior.
    addCandidate(chatJid);
    addCandidate(toJid);
    addCandidate(jidJid);
    addCandidate(fromJid);

    // For LID sessions, include plain PN fallback after chat-based targets.
    if (lidCandidate) {
        addCandidate(botPnJid);
    }

    // Absolute fallback only when upstream omitted all route fields.
    if (!candidates.length) {
        addCandidate(botPnJid);
    }

    return candidates;
}

function parseIntegerEnv(name, defaultValue) {
    const rawValue = process.env[name];
    if (!rawValue) return defaultValue;

    const parsed = Number.parseInt(rawValue, 10);
    if (!Number.isInteger(parsed) || parsed <= 0) {
        throw new Error(`${name} must be a positive integer. Received: "${rawValue}"`);
    }

    return parsed;
}

function getConfig() {
    const phoneNumberRaw = process.env.WA_BOT_PHONE_NUMBER?.trim() || '';
    const phoneNumber = normalizePhoneNumber(phoneNumberRaw);

    if (!phoneNumber) {
        throw new Error(
            'WA_BOT_PHONE_NUMBER is required. Set it in your environment or .env file.',
        );
    }

    if (phoneNumber.length < 8) {
        throw new Error(
            'WA_BOT_PHONE_NUMBER is invalid. Use country code + number, digits only.',
        );
    }

    return {
        host: process.env.HOST || '127.0.0.1',
        port: parseIntegerEnv('PORT', 3001),
        authDir: process.env.WA_AUTH_DIR || './whatsapp-auth',
        pairingDelayMs: parseIntegerEnv('PAIRING_DELAY_MS', 5000),
        waLogLevel: process.env.WA_LOG_LEVEL || 'silent',
        waBotPhoneNumber: phoneNumber,
    };
}

const config = getConfig();
const fastify = Fastify({ logger: true });
await fastify.register(fastifyWebsocket);

let sock;
const activeClients = new Set();
let isPairingInProgress = false;
const sentMessageStore = new Map();
const sentMessageOrder = [];

function rememberMessageById(messageId, messageContent) {
    if (!messageId || !messageContent) return;

    if (!sentMessageStore.has(messageId)) {
        sentMessageOrder.push(messageId);
    }
    sentMessageStore.set(messageId, messageContent);

    while (sentMessageOrder.length > MAX_STORED_MESSAGES) {
        const oldestId = sentMessageOrder.shift();
        if (oldestId) {
            sentMessageStore.delete(oldestId);
        }
    }
}

async function sendTextMessage(targetJid, text) {
    if (!sock) {
        throw new Error('WhatsApp socket is not connected');
    }
    const sent = await sock.sendMessage(targetJid, { text });
    rememberMessageById(sent?.key?.id, sent?.message);
    return sent;
}

function removePicoclawClient(socket, reason, err) {
    activeClients.delete(socket);
    if (reason === 'error') {
        fastify.log.warn(
            { err, activeClients: activeClients.size },
            'Picoclaw websocket client error',
        );
        return;
    }

    fastify.log.info(
        { activeClients: activeClients.size },
        'Picoclaw websocket client disconnected',
    );
}

function broadcastToPicoclaw(payload) {
    if (!activeClients.size) {
        fastify.log.warn(
            'No active Picoclaw websocket clients to receive inbound message',
        );
        return;
    }

    for (const client of activeClients) {
        if (client.readyState === WS_OPEN_STATE) {
            client.send(payload);
        }
    }
}

async function handleOutboundMessage(parsedMessage) {
    const normalizedContent = extractOutboundText(parsedMessage);
    if (!normalizedContent) {
        fastify.log.warn(
            { parsedMessage },
            'Skipping outbound message: missing text content',
        );
        return;
    }

    if (!sock) {
        fastify.log.warn(
            'Skipping outbound message: WhatsApp socket not ready',
        );
        return;
    }

    const targetCandidates = buildTargetCandidates(
        parsedMessage,
        config.waBotPhoneNumber,
    );
    if (!targetCandidates.length) {
        fastify.log.warn(
            { parsedMessage },
            'Skipping outbound message: missing target JID',
        );
        return;
    }

    const outboundText = addSinglePicoPrefix(normalizedContent);
    fastify.log.info(
        {
            targetCandidates,
            outboundLength: outboundText.length,
        },
        'Attempting outbound WhatsApp send',
    );

    let lastError;
    let sentTargetJid = '';

    for (const candidate of targetCandidates) {
        try {
            await sendTextMessage(candidate, outboundText);
            sentTargetJid = candidate;
            break;
        } catch (err) {
            lastError = err;
            fastify.log.warn(
                { candidate, err },
                'Outbound WhatsApp send failed for candidate JID',
            );
        }
    }

    if (!sentTargetJid) {
        fastify.log.error(
            { targetCandidates, err: lastError },
            'Outbound WhatsApp send failed for all target JIDs',
        );
        return;
    }

    fastify.log.info({ sentTargetJid }, 'Outbound WhatsApp send succeeded');
}

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState(config.authDir);

    sock = makeWASocket({
        auth: state,
        logger: pino({ level: config.waLogLevel }),
        printQRInTerminal: false,
        version: WA_VERSION,
        // Keep a known-good desktop fingerprint for this environment.
        browser: ['Ubuntu', 'Chrome', '20.0.04'],
        getMessage: async (key) => sentMessageStore.get(key?.id),
    });

    sock.ev.on('creds.update', saveCreds);

    if (!state.creds.registered && !isPairingInProgress) {
        isPairingInProgress = true;
        console.log(
            `\n⏳ Delaying ${config.pairingDelayMs}ms for socket stabilization...`,
        );

        setTimeout(async () => {
            try {
                const code = await sock.requestPairingCode(config.waBotPhoneNumber);
                const formattedCode = code?.match(/.{1,4}/g)?.join('-') || code;

                console.log(`\n========================================`);
                console.log(`🔑 WHATSAPP PAIRING CODE: ${formattedCode}`);
                console.log(`========================================\n`);
                console.log(
                    'Enter this code in your phone: WhatsApp -> Linked Devices -> Link with phone number instead',
                );
            } catch (err) {
                console.error('❌ Failed to request pairing code:', err?.message || err);
                isPairingInProgress = false;
            }
        }, config.pairingDelayMs);
    }

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

            if (shouldReconnect) {
                console.log(
                    `⚠️ Socket dropped (Code: ${statusCode}). Reconnecting in ${config.pairingDelayMs}ms...`,
                );
                setTimeout(connectToWhatsApp, config.pairingDelayMs);
            } else {
                console.log(
                    '❌ Logged out. Delete the whatsapp-auth folder and restart.',
                );
                isPairingInProgress = false;
            }
        } else if (connection === 'open') {
            isPairingInProgress = false;
            console.log('✅ Baileys Meta socket connected successfully.');
        }
    });

    // INBOUND: WhatsApp -> Fastify -> Picoclaw
    sock.ev.on('messages.upsert', (m) => {
        for (const msg of m.messages || []) {
            rememberMessageById(msg?.key?.id, msg?.message);
        }

        if (m.type !== 'notify') return;

        for (const msg of m.messages) {
            if (!msg.message) continue;

            const content = extractMessageText(msg.message);
            if (!content) continue;
            if (!msg.key?.remoteJid) continue;

            const senderPhone = extractSenderPhone(msg.key);
            const isAllowedSender =
                msg.key.fromMe || senderPhone === config.waBotPhoneNumber;
            if (!isAllowedSender) {
                fastify.log.debug(
                    {
                        senderPhone,
                        allowed: config.waBotPhoneNumber,
                        fromMe: msg.key.fromMe,
                        chat: msg.key.remoteJid,
                    },
                    'Ignoring inbound message from non-allowed sender',
                );
                continue;
            }

            if (hasPicoPrefix(content)) {
                fastify.log.debug(
                    { senderPhone, chat: msg.key.remoteJid, content },
                    'Ignoring inbound [🦞:] -prefixed message to prevent loops',
                );
                continue;
            }

            const picoclawPayload = {
                type: 'message',
                from: msg.key.participant || msg.key.remoteJid,
                chat: msg.key.remoteJid,
                content,
                id: msg.key.id,
                from_name: msg.pushName || 'User',
            };

            const payloadStr = JSON.stringify(picoclawPayload);
            fastify.log.info(
                { chat: picoclawPayload.chat, id: picoclawPayload.id },
                'Forwarding inbound message to Picoclaw',
            );
            broadcastToPicoclaw(payloadStr);
        }
    });
}

// OUTBOUND: Picoclaw -> Fastify -> WhatsApp
fastify.get('/', { websocket: true }, (socket) => {
    activeClients.add(socket);
    fastify.log.info(
        { activeClients: activeClients.size },
        'Picoclaw websocket client connected',
    );

    socket.on('close', () => removePicoclawClient(socket, 'close'));
    socket.on('error', (err) => removePicoclawClient(socket, 'error', err));

    socket.on('message', async (message) => {
        try {
            const parsedMessage = JSON.parse(message.toString());
            await handleOutboundMessage(parsedMessage);
        } catch (err) {
            fastify.log.error(
                { err, raw: String(message || '') },
                'Failed to route outbound message',
            );
        }
    });
});

try {
    await fastify.listen({ port: config.port, host: config.host });
    console.log(`🚀 Fastify WebSocket Bridge running on ws://${config.host}:${config.port}`);
    await connectToWhatsApp();
} catch (err) {
    fastify.log.error(err);
    process.exit(1);
}
