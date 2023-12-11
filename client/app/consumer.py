import json
from channels.generic.websocket import AsyncWebsocketConsumer
from app.engines.initiator import Initiate
 
class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.roomGroupName = "group_chat_gfg"
        await self.channel_layer.group_add(
            self.roomGroupName ,
            self.channel_name
        )
        await self.accept()
    async def disconnect(self , close_code):
        await self.channel_layer.group_discard(
            self.roomGroupName , 
            self.channel_layer 
        )
    async def receive(self, text_data):
        text_data_json = json.loads(text_data)

        user_message = text_data_json["message"]
        username = text_data_json["username"]

        await self.channel_layer.group_send(
            self.roomGroupName, {
                "type": "sendMessage",
                "message": user_message,
                "username": username,
            })

        init = Initiate(user_message)
        result = init.start()
        if result is not None:
            bot_message = result
            bot_username = "ai"

            await self.channel_layer.group_send(
                self.roomGroupName, {
                    "type": "sendMessage",
                    "message": bot_message,
                    "username": bot_username,
                })
    async def sendMessage(self , event) : 
        message = event["message"]
        username = event["username"]
        await self.send(text_data = json.dumps({"message":message ,"username":username}))
