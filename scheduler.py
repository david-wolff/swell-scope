from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from collector import run_collector
import logging
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

def start_scheduler():
    scheduler = BackgroundScheduler()
    # Roda todo dia às 06h da manhã
    scheduler.add_job(run_collector, CronTrigger(hour=6, minute=0))
    scheduler.start()
