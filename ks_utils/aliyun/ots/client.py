import logging
from os import environ
from tablestore import OTSClient, Row, Condition, RowExistenceExpectation, OTSClientError, OTSServiceError, PutRowItem, \
    BatchWriteRowRequest, TableInBatchWriteRowItem, TermQuery, TermsQuery, ColumnReturnType, ColumnsToGet, SearchQuery, \
    NestedQuery, WildcardQuery, ExistsQuery, PrefixQuery, GroupByField, Sort, FieldSort, SortOrder, BoolQuery, RangeQuery

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, instance_name=''):
        self.endpoint = environ.get('OTS_ENDPOINT')
        self.access_id = environ.get('OTS_ACCESS_ID')
        self.access_key = environ.get('OTS_ACCESS_KEY')
        if not self.endpoint or not self.access_id or not self.access_key or not instance_name:
            raise Exception('missing endpoint, access_id, access_key or instance_name')

        self.client = OTSClient(
            self.endpoint,
            self.access_id,
            self.access_key,
            instance_name
        )
        self.__must_query = []
        self.__must_not_query = []
        self.__sort = []

        self.results = None
        self.agg_results = None
        self.group_by_results = None
        self.total_count = 0
        self.next_token = None
        self.is_all_succeed = True

    def search(self, table_name: str, table_index: str, query: dict, limit=50, offset=0, columns_to_get=[]):
        """
        query:
        {
            'must': [
                {
                    'kind': 'term',
                    'condition': ('openid', 'xxxxxx')
                },
                {
                    'kind': 'terms',
                    'condition': ('openid', ['A', 'B'])
                },
                {
                    'kind': 'prefix',
                    'condition': ('openid', 'AA')
                },
                {
                    'kind': 'range',
                    'condition': ('time', '123', '234', True, True)
                },
                {
                    'kind': 'wildcard',
                    'condition': ('name', '*AA*')
                },
            ],
            'must_not': [

            ],
            'sort': [
                {
                    'kind': 'field',
                    'condition': ('time', 'asc')
                },
                {
                    'kind': 'field',
                    'condition': ('time', 'desc')
                },
            ]
        }
        """
        self.__prepare_query(query)
        r = self.client.search(
            table_name=table_name,
            index_name=table_index,
            search_query=self.__get_search_query(limit=limit, offset=offset),
            columns_to_get=ColumnsToGet(column_names=columns_to_get, return_type=ColumnReturnType.SPECIFIED) if columns_to_get else ColumnsToGet(return_type=ColumnReturnType.ALL)
        )
        self.results = r.rows
        self.next_token = r.next_token
        self.total_count = r.total_count
        self.is_all_succeed = r.is_all_succeed

    def get_results(self, with_id: bool = False):
        if not self.results:
            return []

        formatted = []
        for item in self.results:
            tmp = {}
            k = item[0] + item[1] if with_id else item[1]
            for v in k:
                tmp[v[0]] = v[1]
            formatted.append(tmp)

        return formatted

    def get_results_raw(self):
        return self.results

    def get_total_count(self):
        return self.total_count

    def __get_search_query(self, limit=50, offset=0):
        return SearchQuery(
            query=BoolQuery(
                must_queries=self.__must_query,
                must_not_queries=self.__must_not_query
            ),
            sort=self.__get_sort_query(),
            limit=limit,
            offset=offset,
            get_total_count=True,
        )

    def __get_sort_query(self):
        return Sort(
            sorters=self.__sort
        )

    def __prepare_query(self, query):
        for k, v in query.items():
            self.__build_query_terms(k, v)

    def __build_query_terms(self, query_type, terms):
        temp_query = []
        for term in terms:
            if 'kind' not in term:
                return
            if term['kind'] == 'term':
                temp_query.append(TermQuery(*term['condition']))
            if term['kind'] == 'terms':
                temp_query.append(TermsQuery(*term['condition']))
            if term['kind'] == 'prefix':
                temp_query.append(PrefixQuery(*term['condition']))
            if term['kind'] == 'range':
                temp_query.append(RangeQuery(*term['condition']))
            if term['kind'] == 'wildcard':
                temp_query.append(WildcardQuery(*term['condition']))
            if term['kind'] == 'field':
                temp_query.append(FieldSort(
                    term['condition'][0],
                    SortOrder.DESC if str(term['condition'][1]).lower() == 'desc' else SortOrder.ASC
                ))

        if query_type == 'must':
            self.__must_query += temp_query
        if query_type == 'must_not':
            self.__must_not_query += temp_query
        if query_type == 'sort':
            self.__sort += temp_query
