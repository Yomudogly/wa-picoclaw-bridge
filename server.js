import 'dotenv/config';
import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import { makeWASocket, useMultiFileAuthState, DisconnectReason } from '@whiskeysockets/baileys';
import pino from 'pino';

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
    const phoneNumber = phoneNumberRaw.replace(/[^0-9]/g, '');

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

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState(config.authDir);

    sock = makeWASocket({
        auth: state,
        logger: pino({ level: config.waLogLevel }),
        printQRInTerminal: false,
        version: [2, 3000, 1033893291],
        // Bypass crypto rejection on MacOS/Docker
        browser: ['Ubuntu', 'Chrome', '20.0.04'],
    });

    sock.ev.on('creds.update', saveCreds);

    if (!state.creds.registered && !isPairingInProgress) {
        isPairingInProgress = true;
        console.log(`\n⏳ Delaying ${config.pairingDelayMs}ms for socket stabilization...`);

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
                console.log('❌ Logged out. Delete the whatsapp-auth folder and restart.');
                isPairingInProgress = false;
            }
        } else if (connection === 'open') {
            isPairingInProgress = false;
            console.log('✅ Baileys Meta socket connected successfully.');
        }
    });

    // INBOUND: WhatsApp -> Fastify -> Picoclaw
    sock.ev.on('messages.upsert', async (m) => {
        // Log raw event for debugging JID formats in the terminal
        console.log('\n--- RAW BAILEYS EVENT ---');
        console.log(JSON.stringify(m, null, 2));
        console.log('-------------------------\n');

        if (m.type !== 'notify') return;

        for (const msg of m.messages) {
            if (!msg.message) continue;

            // Safely extract text from standard or extended message types
            const content = msg.message.conversation || msg.message.extendedTextMessage?.text || '';

            if (!content) continue;

            const picoclawPayload = {
                type: 'message',
                from: msg.key.participant || msg.key.remoteJid,
                chat: msg.key.remoteJid,
                content,
                id: msg.key.id,
                from_name: msg.pushName || 'User',
            };

            const payloadStr = JSON.stringify(picoclawPayload);
            console.log('➡️ SENDING TO PICOCLAW:', payloadStr);

            for (const client of activeClients) {
                if (client.readyState === 1) client.send(payloadStr);
            }
        }
    });
}

// OUTBOUND: Picoclaw -> Fastify -> WhatsApp
fastify.get('/', { websocket: true }, (socket, req) => {
    activeClients.add(socket);
    socket.on('close', () => activeClients.delete(socket));

    socket.on('message', async (message) => {
        try {
            const parsedMessage = JSON.parse(message.toString());
            fastify.log.info('⬅️ RECEIVED FROM PICOCLAW:', parsedMessage);

            if (parsedMessage.type === 'message' && sock) {
                // Ensure target JID uses 'to' or falls back to 'chat'
                const targetJid = parsedMessage.to || parsedMessage.chat;
                if (targetJid) {
                    await sock.sendMessage(targetJid, {
                        text: parsedMessage.content,
                    });
                } else {
                    fastify.log.warn('Outbound message missing target JID');
                }
            }
        } catch (err) {
            fastify.log.error('Failed to route outbound message', err);
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
