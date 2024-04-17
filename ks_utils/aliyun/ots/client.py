import logging
from os import environ

from tablestore import OTSClient, TermQuery, TermsQuery, ColumnReturnType, ColumnsToGet, SearchQuery, \
    WildcardQuery, PrefixQuery, Sort, FieldSort, SortOrder, BoolQuery, \
    RangeQuery, Count, DistinctCount, Sum, Avg, Max, Min, GroupByFilter, Collapse

logger = logging.getLogger(__name__)


class Client:
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
        ],
        'agg': [
            {
                'kind': 'count',
                'condition': ('field_name or *',),
                'kwargs': {
                    'name': 'max_by_field_name'
                }
            },
            {
                'kind': 'distinct_count',
                'condition': ('field_name',),
                'kwargs': {
                    'name': 'distinct_count_by_field_name',
                    'missing_value': 'a'
                }
            },
            {
                'kind': 'sum',
                'condition': ('field_name',),
                'kwargs': {
                    'name': 'sum_by_field_name'
                }
            },
            {
                'kind': 'avg',
                'condition': ('field_name',),
                'kwargs': {
                    'name': 'avg_by_field_name'
                }
            },
            {
                'kind': 'max',
                'condition': ('field_name',),
                'kwargs': {
                    'name': 'max_by_field_name'
                }
            },
            {
                'kind': 'min',
                'condition': ('field_name',),
                'kwargs': {
                    'name': 'min_by_field_name'
                }
            }
        ],
        'group_by': [
            {
                'kind': 'group_by_filter',
                'name': 'xxxx',
                'filters': [
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
                'sub_agg': [
                    {
                        'kind': 'count',
                        'condition': ('field_name or *',),
                        'kwargs': {
                            'name': 'max_by_field_name'
                        }
                    },
                    {
                        'kind': 'distinct_count',
                        'condition': ('field_name',),
                        'kwargs': {
                            'name': 'distinct_count_by_field_name',
                            'missing_value': 'a'
                        }
                    },
                    {
                        'kind': 'sum',
                        'condition': ('field_name',),
                        'kwargs': {
                            'name': 'sum_by_field_name'
                        }
                    },
                    {
                        'kind': 'avg',
                        'condition': ('field_name',),
                        'kwargs': {
                            'name': 'avg_by_field_name'
                        }
                    },
                    {
                        'kind': 'max',
                        'condition': ('field_name',),
                        'kwargs': {
                            'name': 'max_by_field_name'
                        }
                    },
                    {
                        'kind': 'min',
                        'condition': ('field_name',),
                        'kwargs': {
                            'name': 'min_by_field_name'
                        }
                    }
                ]
            }
        ]
    }
    """

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
        self._raw_query = None
        self._must_query = []
        self._must_not_query = []
        self._sort_query = []
        self._agg_query = []
        self._group_by_query = []
        self._collapse = None

        self.results = None
        self.agg_results = None
        self.group_by_results = None
        self.total_count = 0
        self.next_token = None
        self.is_all_succeed = True

    def search(self, table_name: str, table_index: str, query: dict, limit=50, offset=0, get_total_count=False, columns_to_get=[]):
        self.__prepare_query(query)
        r = self.client.search(
            table_name=table_name,
            index_name=table_index,
            search_query=self.__get_search_query(limit=limit, offset=offset, get_total_count=get_total_count),
            columns_to_get=ColumnsToGet(column_names=columns_to_get, return_type=ColumnReturnType.SPECIFIED) if columns_to_get else ColumnsToGet(return_type=ColumnReturnType.ALL),
        )
        self.results = r.rows
        self.next_token = r.next_token
        self.total_count = r.total_count
        self.is_all_succeed = r.is_all_succeed
        self.agg_results = r.agg_results
        self.group_by_results = r.group_by_results

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

    def get_agg_results(self):
        result = []
        for agg in self.agg_results:
            result.append({
                'name': agg.name,
                'value': agg.value
            })
        return result

    def get_group_by_results(self):
        result = []
        for group_by in self.group_by_results:
            group_query = next((sub for sub in self._raw_query['group_by'] if sub['name'] == group_by.name))
            filters = group_query['filters']
            group_data = []
            i = 0
            for item in group_by.items:
                tmp = {
                    'name': filters[i]['name'],
                    'total': item.row_count
                }
                tmp_agg = []
                for agg in item.sub_aggs:
                    tmp_agg.append({
                        'name': agg.name,
                        'value': agg.value
                    })
                if tmp_agg:
                    tmp['agg'] = tmp_agg

                group_data.append(tmp)
                i += 1

            result.append({
                'name': group_by.name,
                'value': group_data
            })
        return result

    def get_results_raw(self):
        return self.results

    def get_total_count(self):
        return self.total_count

    def __get_search_query(self, limit=50, offset=0, get_total_count=False):
        query = {
            'query': BoolQuery(
                must_queries=self._must_query,
                must_not_queries=self._must_not_query
            ),
            'limit': limit,
            'offset': offset,
            'get_total_count': get_total_count,
        }
        if self._sort_query:
            query['sort'] = self.__get_sort_query()
        if self._agg_query:
            query['aggs'] = self._agg_query
        if self._group_by_query:
            query['group_bys'] = self._group_by_query
        if self._collapse:
            query['collapse_field'] = self._collapse
        return SearchQuery(**query)

    def __get_sort_query(self):
        return Sort(
            sorters=self._sort_query
        )

    def __prepare_query(self, query):
        self._raw_query = query
        for k, v in query.items():
            self.__build_query_terms(k, v)

    def __build_query_terms(self, query_type, terms):
        if query_type == 'collapse':
            self._collapse = Collapse(terms)
            return

        term_mapping = {
            'term': TermQuery,
            'terms': TermsQuery,
            'prefix': PrefixQuery,
            'range': RangeQuery,
            'wildcard': WildcardQuery,
            'field': FieldSort,
            'count': Count,
            'distinct_count': DistinctCount,
            'sum': Sum,
            'avg': Avg,
            'max': Max,
            'min': Min,
            'group_by_filter': GroupByFilter
        }

        temp_query = []
        for term in terms:
            if 'kind' not in term:
                return
            if term['kind'] in term_mapping:
                if term['kind'] == 'field':
                    temp_query.append(term_mapping[term['kind']](
                        term['condition'][0],
                        SortOrder.DESC if str(term['condition'][1]).lower() == 'desc' else SortOrder.ASC
                    ))
                elif term['kind'] == 'group_by_filter':
                    filters = []
                    sub_aggs = []
                    for item in term['filters']:
                        filters.append(term_mapping[item['kind']](*item['condition']))
                    for item in term['sub_agg']:
                        sub_aggs.append(term_mapping[item['kind']](*item['condition'], **item['kwargs']))
                    temp_query.append(GroupByFilter(filters, name=term['name'], sub_aggs=sub_aggs))
                else:
                    if 'kwargs' in term:
                        temp_query.append(term_mapping[term['kind']](*term['condition'], **term['kwargs']))
                    else:
                        temp_query.append(term_mapping[term['kind']](*term['condition']))

        if query_type in ['must', 'must_not', 'sort', 'agg', 'group_by']:
            setattr(self, f'_{query_type}_query', temp_query)
