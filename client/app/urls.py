from django.urls import path
from app import views as app_views
 
 
urlpatterns = [
    path("", app_views.index, name="index"),
]
