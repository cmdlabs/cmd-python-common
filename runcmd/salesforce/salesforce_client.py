"""
Salesforce client
"""
from abc import abstractmethod
import requests
from flatten_json import flatten
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)


class SalesforceClient(object):
    """
    Calling salesforce APIs to get table data
    """

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_org_id(self):
        """
        Get organization ID to identify different salesforce client
        """
        pass

    @abstractmethod
    def _get_soqls(self):
        # "Private" method (should only be accessed from with instance)
        """
        soql - Salesforce Object Query Language
        get the Dictionary of {tableName : soql}
        """
        pass

    @abstractmethod
    def revise_conflict_table_columns(self):
        """
        Sove the conflict tables with changed name
        """
        pass

    def query_data_from_api(self, instance, username, password, security_token):
        """
        Query the salesforce API using SOQL
        return {tableName:recordsData}
        """
        sf_client = Salesforce(
            instance=instance,
            username=username,
            password=password,
            security_token=security_token,
            session=requests.Session())

        dic_table_records = {}
        for soql in self._get_soqls().items():
            # print(soql)

            # Potential big issue ! If the result is big data, easy to get out of memory
            # Should use https://github.com/springml/spark-salesforce
            try:
                result = sf_client.query_all(soql[1])
                data = [flatten(r) for r in result['records']]
                dic_table_records[soql[0]] = data
            except SalesforceMalformedRequest as sf_ex:
                LOG.info(">>> Querying table %s failed and skipped!" %
                         soql[0] + sf_ex.content)
        return dic_table_records
