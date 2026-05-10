import {randomUUID, randomBytes, createHash} from "crypto"
import DateTools from "@hackthedev/datetools"
import AuthTools from "@hackthedev/dsync-auth";

function generateRandomString() {
    return (Math.random().toString(36).slice(2)) + (Math.random().toString(36).slice(2))
}

export default class dSyncInbox {
    constructor({
                    io = null,
                    app = null,
                    express = null,
                    dSyncSign = null,
                    dSyncSql = null,
                    dSyncAuth = null,
                    isValidated = null,
                    getIdentifier = null,
                    beforeReturn = null,
                } = {}) {

        this.io = io;
        this.signer = dSyncSign;
        this.db = dSyncSql;
        this.express = express
        this.auther = dSyncAuth

        this.isValidated = typeof isValidated === "function" ? isValidated : null;
        this.getIdentifier = typeof getIdentifier === "function" ? getIdentifier : null;
        this.beforeReturn = typeof beforeReturn === "function" ? beforeReturn : null;

        if(!isValidated) throw new Error("No isValidated function provided");
        if(!getIdentifier) throw new Error("No getIdentifier function provided");

        if (!io) {
            console.error("socket io is required!")
            process.exit(0)
        }

        if (!app) {
            console.error("Express app is required!")
            process.exit(0)
        }

        if (!express) {
            console.error("Express is required!")
            process.exit(0)
        }

        if (!dSyncSign) {
            console.error("dSyncSign is required!")
            process.exit(0)
        }

        if (!dSyncAuth) {
            console.error("dSyncAuth is required!")
            process.exit(0)
        }

        if (!dSyncSql) {
            console.error("dSyncSql is required!")
            process.exit(0)
        }

        app.use((req, res, next) => {
            const origin = req.headers.origin;

            res.header("Access-Control-Allow-Origin", "*");
            res.header("Vary", "Origin");
            res.header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
            res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
            res.header("Access-Control-Max-Age", "86400");
            res.set("Cache-Control", "no-store");

            if (req.method === "OPTIONS") {
                return res.sendStatus(204);
            }

            next();
        });

        app.post(`/inbox/fetch{/:timestamp}{/:inboxId}{/:customId}`, this.express.json(), async (req, res) => {
            const {inboxId, timestamp, customId} = req?.params;
            if(!await this.isValidated(req, res)) return res.status(403).json({ error: "Forbidden" })

            let identifier = await this.getIdentifier(req, res);
            if(!identifier) return res.status(403).json({ error: "Missing identifier" })

            let messages = null;
            if(!inboxId && !timestamp) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE targetId = ? ORDER BY createdAt DESC LIMIT 50",
                    [identifier]
                );
            }
            if(!inboxId && timestamp) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE identifier = ? created < ? ORDER BY createdAt DESC LIMIT 50",
                    [identifier, timestamp]
                );
            }
            if(inboxId && !customId) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE id = ? AND identifier = ? LIMIT 50",
                    [inboxId, identifier]
                );
            }
            if(!inboxId && customId) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE customId = ? AND identifier = ? LIMIT 50",
                    [inboxId, identifier]
                );
            }

            if(messages?.length > 0){
                for(let message of messages){
                    if(message?.data?.startsWith("{") && typeof message?.data === "string") message.data = JSON.parse(message.data);
                }
            }

            if(this.beforeReturn) await this.beforeReturn(req, res, messages)

            try {
                res.status(200).json({ inbox: messages });
            } catch (err) {
                res.status(400).json({error: "Failed to solve challenge"})
            }
        })

        this.io.on("connection", async (socket) => {
            // socket ip
            var ip = this.getSocketIp(socket);

            socket.on("/messenger/hello", async (user, response) => {
                if(!await this.validateSocketAuth(user?.sessionId, user?.publicKey, response)) return;

                let userGid = this.signer.generateGid(user?.publicKey);
                if(!userGid) return response({ error: "Failed to generate gid" })

                // join own room to emit messages to
                const targetIsOnline = io.sockets.adapter.rooms.has(userGid);
                if(!targetIsOnline) {
                    socket.join(userGid);
                    console.log(`user joined ${userGid}`)
                }

                response({ error: null})
            })

            socket.on("/messenger/send", async (user, response) => {
                if(!await this.validateSocketAuth(user?.sessionId, user?.message?.publicKey, response)) return;

                if(!user?.message || typeof user?.message !== "object") return response({ error: "No message object provided" })
                if(!user?.message?.type) return response({ error: "No message type provided" })

                if(!user?.message?.publicKey) return response({ error: "No public key provided" })
                if(!user?.message?.targetPublicKey) return response({ error: "No target public key provided" })

                let userGid = this.signer.generateGid(user?.message?.publicKey);
                let targetGid = this.signer.generateGid(user?.message?.targetPublicKey);

                const targetIsOnline = io.sockets.adapter.rooms.has(targetGid);
                console.log(targetIsOnline)

                // if target is online send it directly to them
                this.emitToGid(targetGid, "/messenger/receive", user.message);

                // temporarily save the message
                this.setInboxEntry({
                    targetId: targetGid,
                    type: "messenger_user-message",
                    data: user.message,
                    expiresAt: DateTools.getDateFromOffset("3 days").getTime(),
                    customId: user.message.timestamp
                })


                response({ error: null})
            })

            socket.on("/messenger/fetch", async (user, response) => {
                if (!await this.validateSocketAuth(user?.sessionId, user?.message?.publicKey, response)) return;
            });
        })
    }

    async emitToGid(targetGid, event, data){
        this.io.to(`${targetGid}`).emit(event, {...data})
    }

    async validateSocketAuth(sessionId, publicKey, response){
        if(!sessionId || !publicKey){
            response?.({ error: "Authentication failed" })
            return false
        }

        let sessionResult = AuthTools.verifySession(this.auther.authSessions, sessionId, publicKey);
        let result = sessionResult?.valid ?? false;

        if(!result && response){
            response({ error: "Authentication failed - Result invalid" })
            return false;
        }
        else if(result === true){
            return true;
        }

        response({ error: "Authentication failed" })
        return false;
    }

    getSocketIp(socket){
        return socket?.handshake?.headers["x-forwarded-for"]?.split(",")[0].trim()
            || socket?.handshake?.headers["x-real-ip"]
            || socket?.handshake?.address;
    }

    async setInboxEntry({
        targetId = null,
        type = null,
        data = null,
        isRead = null,
        customId = null,
        expiresAt = null,
    } = {}){
        if(!type?.trim()) throw new Error("type is required!")
        if(!targetId?.trim()) throw new Error("targetId is required!")
        if(!data || typeof data !== "object") throw new Error("data is required and must be a json object!")
        if(!customId) throw new Error("customId is required for identification!")

        return await this.db.queryDatabase(
            `INSERT INTO inbox (id, targetId, type, data, isRead, customId, expiresAt)
             VALUES(?, ?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                  isRead = IF(isRead IS NULL, VALUES(isRead), isRead)`,
            [randomUUID(), targetId, type, JSON.stringify(data, null, 4), isRead, customId, expiresAt]
        );
    }

    async init(){
        const tables = [
            {
                name: "inbox",
                columns: [
                    {name: "rowId", type: "int(100) NOT NULL AUTO_INCREMENT PRIMARY KEY"},
                    {name: "id", type: "varchar(255) NOT NULL"},
                    {name: "customId", type: "varchar(500) NULL DEFAULT NULL"},
                    {name: "targetId", type: "varchar(500) NOT NULL"},
                    {name: "type", type: "varchar(255) NOT NULL"},
                    {name: "data", type: "longtext"},
                    {name: "createdAt", type: "bigint NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000)"},
                    {name: "expiresAt", type: "bigint NULL DEFAULT NULL"},
                    {name: "isRead", type: "bigint NULL DEFAULT NULL"},
                ]
            }
        ]

        for (const table of tables) {
            await this.db.checkAndCreateTable(table);
        }
    }
}
