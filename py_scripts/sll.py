import json
from dsp.dspapi_entity import DspApiConnection
from helpers.loggingHelper import Logger
from urllib.error import URLError, HTTPError
from urllib.parse import parse_qs
from urllib.request import Request, urlopen

# The Slack channel to send a message to stored in the slackChannel environment variable
SLACK_CHANNEL = "#alert"
# SLACK_CHANNEL = "@simon.volman"  # for debug

HOOK_URL = "https://hooks.slack.com/services/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"


# state = good, warning, danger
def postMessageIntoSlack(alarm_name, state):
    slack_message = {
        'channel': SLACK_CHANNEL,
        'attachments': [{
            'color': state,  # good, warning, danger
            'text': alarm_name

            #  actions': [
            #     {
            #         "name": "action",
            #         "text": "DoNothing",
            #         "type": "button",
            #         "value": "DoNothing"
            #     },
            #     {
            #         "name": "action",
            #         "text": "DoSomething",
            #         "style": "danger",
            #         "type": "button",
            #         "value": "DoSomething",
            #         "confirm": {
            #             "title": "Are you sure?",
            #             "text": "Wouldn't you prefer to do nothing?",
            #             "ok_text": "Yes",
            #             "dismiss_text": "No"
            #         }
            #     }]
        }]
    }

    logger = Logger('DEBUG')

    req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to {}".format(SLACK_CHANNEL))
    except HTTPError as e:
        logger.error("Request failed: {} {}".format(e.code, e.reason))
    except URLError as e:
        logger.error("Server connection failed: {}".format(e.reason))

    return


def lambda_handler(event, context):
    logger = Logger('DEBUG')

    # postMessageIntoSlack("Test Alarm Message from Amazon", "good")

    query = event.get("body")
    mm = parse_qs(query)
    teamdomain = mm.get("team_domain", [""])[0]

    if (teamdomain != "inventale"):
        return "domain not appropriate: " + teamdomain;

    user_name = mm.get("user_name", [""])[0]

    try:
        logger.info('Init connections:')
        dspapi_connection = DspApiConnection('maxifier.flight.getList', 1, logger)
        logger.info('Amount of available flights in ATD is {}\n'.format(dspapi_connection.amount))
        logger.info('Flight fields:')
        for key in dspapi_connection.data.keys():
            logger.info('{}: {}'.format(key, dspapi_connection.data.get(key)))

        # logger.info('Post status into Slack')
        postMessageIntoSlack(
            str(user_name) + ': DSP Connection is ok. Flights in ATD: {}'.format(dspapi_connection.amount), "good")

        return "ok"

    except ValueError as e:
        logger.error("DSP connection failed")
        postMessageIntoSlack("DSP Connection failed", "danger")

    return
