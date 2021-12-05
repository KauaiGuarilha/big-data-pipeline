from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator

class ChapecoenseAirflowPlugin(AirflowPlugin):
    name = "chapecoense"
    operators = [TwitterOperator]
