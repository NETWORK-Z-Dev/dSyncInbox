# dSyncInbox

This library is the core of the decentralized messenger feature found in DCTS. It uses a few different libraries too, like dSyncSign, dSyncSql and more!

```js
import dSyncInbox from "@hackthedev/dsync-inbox"


// define the actual library now
let inbox = new dSyncInbox({
        io,										// socket.io
        app,									// express
        express,								// express
        dSyncSign: signer,						// dSyncSign
        dSyncSql: db,							// dSyncSql
        dSyncAuth: auther,						// dSyncAuth
        isValidated: async (req, res) => {
            // custom callback code
            const {inboxId, timestamp, customId} = req?.params;
            const { id, token, sessionId, publicKey } = req.body;

            if(serverconfig.servermembers[id]?.token === token && !sessionId) return true;

            if(sessionId){
                let sessionResult = dSyncAuth.verifySession(auther.authSessions, sessionId, publicKey);
                return sessionResult?.valid ?? false;
            }

            return false;
        },
        getIdentifier: async (req, res) => {
            // custom callback code
            const {inboxId, timestamp, customId} = req?.params;
            let { id, token, sessionId, publicKey } = req.body;

            if(!id && !token && publicKey){
                let member = await getMemberFromKey(publicKey);
                if (member){
                    id = member.id;
                    token = member.token;
                }
            }

            return id ?? null;
        },
        beforeReturn: async (req, res, inbox) => {
            // custom callback code
            if(Array.isArray(inbox) && inbox.length > 0){
                for(let item of inbox){
                    let itemType = item?.type;

                    // chat mentions
                    if(itemType === "mention"){
                        let messageId = item?.data?.messageId;
                        if(!messageId || messageId?.length !== 12) continue;

                        item.data = await getMessageObjectById(messageId);
                    }
                }
            }
        }
    })

    await inbox.init();
```

