from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from udacity_plugin.operators.load_fact import LoadFactOperator
from udacity_plugin.operators.stage_redshift import StageToRedshiftOperator
from udacity_plugin.operators.load_dimension import LoadDimensionOperator
from udacity_plugin.operators.data_quality import DataQualityOperator
from udacity_plugin.helpers.sql_queries import SqlQueries


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator
    ]
    helpers = [
        SqlQueries
    ]
