from django.urls import path , include
from app.consumer import ChatConsumer
 
websocket_urlpatterns = [
    path("" , ChatConsumer.as_asgi()) , 
]
