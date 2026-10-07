from django.contrib import admin

from log_analyzer.models import *

# Register your models here.


admin.site.register(LogJob)
admin.site.register(BlockFeature)
admin.site.register(JobEvaluation)




@admin.register(BlockPrediction)
class BlockPredictionAdmin(admin.ModelAdmin):
    list_display = ("id", "feature", "label", "anomaly_score", "created_at")
    search_fields = ("=id", "feature__block_id")
    list_filter = ("label",)
    list_select_related = ("feature",)


