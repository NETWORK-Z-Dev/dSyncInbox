import {randomUUID, randomBytes, createHash} from "crypto"
import DateTools from "@hackthedev/datetools"

function generateRandomString() {
    return (Math.random().toString(36).slice(2)) + (Math.random().toString(36).slice(2))
}

export default class dSyncInbox {
    constructor({
                    app = null,
                    express = null,
                    dSyncSign = null,
                    dSyncSql = null,
                    isValidated = null,
                    getIdentifier = null
                } = {}) {

        this.signer = dSyncSign;
        this.db = dSyncSql;
        this.express = express

        this.isValidated = typeof isValidated === "function" ? isValidated : null;
        if(!isValidated) throw new Error("No isValidated function provided");

        this.getIdentifier = typeof getIdentifier === "function" ? getIdentifier : null;
        if(!getIdentifier) throw new Error("No getIdentifier function provided");

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
                    "SELECT * FROM inbox WHERE id = ? AND identifier = ? LIMIT 1",
                    [inboxId, identifier]
                );
            }
            if(!inboxId && customId) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE customId = ? AND identifier = ? LIMIT 1",
                    [inboxId, identifier]
                );
            }

            if(messages?.length > 0){
                for(let message of messages){
                    if(message?.data?.startsWith("{") && typeof message?.data === "string") message.data = JSON.parse(message.data);
                }
            }

            try {
                res.status(200).json({ inbox: messages });
            } catch (err) {
                res.status(400).json({error: "Failed to solve challenge"})
            }
        })
    }

    async setInboxEntry({
        targetId = null,
        type = null,
        data = null,
        isRead = null,
        customId = null,
    } = {}){
        if(!type?.trim()) throw new Error("type is required!")
        if(!targetId?.trim()) throw new Error("targetId is required!")
        if(!data || typeof data !== "object") throw new Error("data is required and must be a json object!")
        if(!customId) throw new Error("customId is required for identification!")

        return await this.db.queryDatabase(
            `INSERT INTO inbox (id, targetId, type, data, isRead, customId)
             VALUES(?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                  isRead = IF(isRead IS NULL, VALUES(isRead), isRead)`,
            [randomUUID(), targetId, type, JSON.stringify(data, null, 4), isRead, customId]
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
                    {name: "isRead", type: "bigint NULL DEFAULT NULL"},
                ]
            }
        ]

        for (const table of tables) {
            await this.db.checkAndCreateTable(table);
        }
    }
}
