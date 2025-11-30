import axios from "axios";
import layerUtils from "/opt/nodejs/utils.js";
import WebSocketFlags from "/opt/nodejs/WebsocketFlags.js";
import WebsocketUtils from "/opt/nodejs/WebsocketUtils.js";
import knex from "/opt/nodejs/db.js";
import DatabaseTableConstants from "/opt/nodejs/DatabaseTableConstants.js";
import { SchedulerClient, DeleteScheduleCommand } from "@aws-sdk/client-scheduler";

const eventBridgeClient = new SchedulerClient({ region: "us-east-2" });

export const handler = async (event) => {
    console.log(`🚀 Upload citation lookup result Event: ${JSON.stringify(event)}`);
    
    try {
        const { campaign_id, scheduleName, organizationId } = event.detail || event;

        if (!campaign_id || !scheduleName || !organizationId) {
            throw new Error('Missing required parameters: campaign_id, scheduleName, organizationId');
        }

        const url = `https://api.brightlocal.com/manage/v1/citation-builder/${campaign_id}/lookup`;
        const apiKey = await layerUtils.getBrightLocalApiKey();

        console.log("Getting Campaign Lookup data");

        const response = await axios.get(url, {
            headers: {
                "Content-Type": "application/json",
                "x-api-key": `${apiKey}`,
            },
        });

        const lookup_status = response.data.lookup_status;
        if (!lookup_status) {
            throw new Error('Invalid response from BrightLocal API: missing lookup_status');
        }

        const saveData = { lookup_status };

        console.log("Campaign Lookup Status:", lookup_status);

        if (lookup_status === "complete") {
            saveData.lookup_completed_at = response.data.lookup_completed_at;
            saveData.citations = JSON.stringify(response.data.citations);

            console.log("Lookup complete");
        } else {
            console.log("Lookup not complete, schedule will remain for next check");
        }

        console.log("Response data to save:", saveData);

        await knex(DatabaseTableConstants.CITATION_CAMPAIGN_TABLE)
            .where({ campaign_id })
            .update(saveData);
        console.log("✅ Successfully processed upload citation lookup result");

        if (lookup_status === "complete") {
            const deleteCommand = new DeleteScheduleCommand({ Name: scheduleName });
            await eventBridgeClient.send(deleteCommand);
            console.log(`🗑️ Successfully deleted schedule: ${scheduleName}`);

            await WebsocketUtils.broadcastWebsocketMessageToOrganization(
                organizationId,
                WebSocketFlags.CITATION_LOOKUP_SUCCESS,
                { campaign_id, lookup_status },
                response,
            );
            console.log(`✅ Successfully broadcasted websocket message for organization: ${organizationId}`);
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Successfully processed upload citation lookup result'
            })
        };

    } catch (error) {
        console.error(`❌ Error processing upload citation lookup result:`, error);
        
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Failed to process upload citation lookup result',
                error: error.message
            })
        };
    }
};
