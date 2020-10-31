"""
hold the flow until some criteria is met

triggers may be changed to be senors - 

wait_for_file(filename) > file_to_json > filter_columns > save_to_file
"""

## from an airflow document
##
class ConversionRatesSensor(BaseSensorOperator):
    """
    An example of a custom Sensor. Custom Sensors generally overload
    the `poke` method inherited from `BaseSensorOperator`
    """
    def __init__(self, *args, **kwargs):
        super(ConversionRatesSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        print 'poking {}'.__str__()
        
        # poke functions should return a boolean
        return check_conversion_rates_api_for_valid_data(context)