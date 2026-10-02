from django.contrib import admin

from log_analyzer.models import *

# Register your models here.
admin.site.register(LogJob)
admin.site.register(BlockFeature)
admin.site.register(BlockPrediction)