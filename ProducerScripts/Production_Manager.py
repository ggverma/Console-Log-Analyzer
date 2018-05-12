from Logger import Logger as LGR
from threading import Lock, Thread
from AWS_CREDS import Kinesis

Logger = LGR(Lock(), '../LogFiles/Producer/Logs-', 'log_file_')
LOG_FILE_PREFIX = "log_"

from producer_meetups import Producer_Meetups as ProducerMeetups
from producer_reddit import Producer_Reddit as ProducerReddit
from producer_twitter import Producer_Twitter as ProducerTwitter

try:
    objKinesis = Kinesis(Logger)

    PM, PR, PT = ProducerMeetups(Logger, objKinesis), ProducerReddit(Logger, objKinesis), ProducerTwitter(Logger, objKinesis, MULTIPLE_POST_SEND_LIMIT = 100)

    reddit_thread = Thread(target = PR.run)
    print("Running Reddit production stream...")

    twitter_thread = Thread(target = PT.run)
    print("Running Twitter production stream...")

    meetups_thread = Thread(target = PM.run)
    print("Running Meetup production stream...")

    reddit_thread.start()
    twitter_thread.start()
    meetups_thread.start()

    reddit_thread.join()
    twitter_thread.join()
    meetups_thread.join()

except Exception as e:
    Logger.log("An error occurred. Exception: " + str(e))
