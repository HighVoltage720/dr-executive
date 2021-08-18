from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from sqlalchemy import text
from datetime import datetime
import yaml
import numpy as np
from pytz import timezone
tz = timezone('EST')

default_args = {
    'owner': 'QE/Siddharth',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 26)
}


def func_perf_regr_smoke_data_collection_function():
    # This notebook will get today's functional smoke, performance smoke and functional regression data from Elasticsearch
    # and calculate PASS % from the numbers

    # This client was designed as very thin wrapper around Elasticsearch’s REST API to allow for maximum flexibility. 
    # This means that there are no opinions in this client; 

    from elasticsearch import Elasticsearch
    from datetime import datetime

    # Python functionalized code
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services 
    ########################################## 
    # Taking the code from "EXECUTIVE_DASHBOARD_data_collection_Functional_Performance_smoke_data_Regression_ver2_IF_ELSE" and 
    #putting it into preliminary large functions for easy orchestration

    # Generalized printing function
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services 
    ########################################## 
    def print_header(header):
        print("==============================================================")
        print(header)
        print("==============================================================")


    import yaml

    with open('/home/Performance_System_analysis/config_dr.yaml') as stream:
        config_details = yaml.load(stream)
        ES_ADDRESS = config_details["elk"]["ES_ADDRESS"]
        ES_INDEX = config_details["elk"]["ES_INDEX"]
        ES_USERNAME = config_details["elk"]["ES_USERNAME"]
        ES_PASSWORD = config_details["elk"]["ES_PASSWORD"]
    
    # Closing the YAML stream for possible further reuse:
    print(stream)
    stream.close()

    # Section to import PASS, FAIL and CANCELLED status items from yaml file
    with open('elk_test_status_dr.yaml') as stream:
        config_details = yaml.load(stream)
        pass_status = config_details["test_status"]["pass_status"]
        fail_status = config_details["test_status"]["fail_status"]
        cancel_status = config_details["test_status"]["cancel_status"]
        skip_status = config_details["test_status"]["skip_status"]
        
    # Closing the YAML stream for possible further reuse:
    print(stream)
    stream.close()    


    ########################################## 
    # Another attempt at CONNECT ;)
    ########################################## 
    # Connect To ElasticSearch
    elastic_client = Elasticsearch(
        [ES_ADDRESS],
        http_auth = (ES_USERNAME, ES_PASSWORD),
        use_ssl = True,
        verify_certs = False,
    )

    import os

    # detect the current working directory and print it
    path = os.getcwd()
    print ("The current working directory is %s" % path)


    ####################### Functional smoke section #######################

    # Querying the Functional PASS/FAIL rates from functional smoke tests for the EXECUTIVE DASHBOARD
    # at the Vertical level over tests ran today
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services
    ########################################## 
    # counting how many documents belong to testcases for the Government application
    print_header( "fields.product.vertical: Business_Services" )

    vertical_name = "Business_Services"

    filter_query = {
        "aggs": {
            "2": {
            "terms": {
                "field": "fields.product.vertical",
                "order": {
                "1": "desc"
                },
                "size": 10
            },
            "aggs": {
                "1": {
                "cardinality": {
                    "field": "testcase.name.keyword"
                }
                },
                "3": {
                "terms": {
                    "field": "testcase.status.keyword",
                    "order": {
                    "1": "desc"
                    },
                    "size": 3
                },
                "aggs": {
                    "1": {
                    "cardinality": {
                        "field": "testcase.name.keyword"
                    }
                    }
                }
                }
            }
            }
        },
        "size": 0,
        "stored_fields": [
            "*"
        ],
        "script_fields": {},
        "docvalue_fields": [
            {
            "field": "@timestamp",
            "format": "date_time"
            },
            {
            "field": "event.created",
            "format": "date_time"
            }
        ],
        "_source": {
            "excludes": []
        },
        "query": {
            "bool": {
            "must": [],
            "filter": [
                {
                "match_all": {}
                },
                {
                "match_phrase": {
                    "fields.Test.environment": "DR"
                }
                },
                {
                "match_phrase": {
                    "fields.product.vertical": "Business_Services"
                }
                },
                {
                "match_phrase": {
                    "fields.Test.type": "functional_smoke"
                }
                },
                {
                "range": {
                    "@timestamp": {
                    "gte": "now-24h",
                    "lte": "now",
                    "format": "strict_date_optional_time"
                    }
                }
                }
            ],
            "should": [],
            "must_not": []
            }
        }
    }

    response = elastic_client.search(index = ES_INDEX, body = filter_query, size = 10)

    print("Got %d Hits:" % response['hits']['total']['value'])

    #Examining the response object
    #print("Got %d Hits:" % response['hits']['total']['value'])
    #print("Got %s Hit relation:" % response['hits']['total']['relation'])

    #Storing the number of hits for looping through list
    number_hits = int(response['hits']['total']['value'])
    print("Number of hits = %d" % number_hits)

    func_testcase_PASS_percent = 0

    if number_hits > 0:
        #Examining the aggregations object
        print_header("'fields.product.vertical': 'Business_Services'")

        #Since the response hits is a list type of object
        list_aggregations_index_identifier = response['aggregations']['2']
        #print(list_aggregations_index_identifier)
        #print(type(list_aggregations_index_identifier))
        
        #Examining the list_aggregations_index_identifier list object and extracting its necessary elements
        #print_header("First element of list of Elasticsearch hits")
        #print(list_aggregations_index_identifier['buckets'])
        #print(type(list_aggregations_index_identifier['buckets']))

        #print_header("buckets key and its nested key/value pairs:")
        buckets_document_list = list_aggregations_index_identifier['buckets']
        #print(buckets_document_list)

        #print_header("First element of buckets_document_list contains bucket dictionary")
        doc_source_key = buckets_document_list[0]
        #for key, value in doc_source_key.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key[key]))

        #Extracting the "buckets" bucket from inside buckets_document_list since that has PASS/FAIL numbers
        #print_header("Third element [3] of buckets_document_list contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key['3']
        #for key, value in doc_source_key_flag_buckets.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key_flag_buckets[key]))

        #Extracting the "buckets" bucket from inside doc_source_key_flag_buckets since that has PASS/FAIL numbers
        print_header("doc_source_key_flag_buckets contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key_flag_buckets['buckets']
        print(doc_source_key_flag_buckets)
        print(type(doc_source_key_flag_buckets))

        loop_counter = 0

        for item in doc_source_key_flag_buckets:
            print(item)
            print(type(item))
            loop_counter = loop_counter + 1
            
        print("\nsize of doc_source_key_flag_buckets list = %d" % len(doc_source_key_flag_buckets))   

        # Looping through the elements of doc_source_key_flag_buckets to get the PASS and FAIL numbers from the buckets

        # general purpose loop counter
        loop_counter = 1

        # Initializing all PASS/FAIL/CANCELLED variables to 0
        PASS_key_value_num = 0
        FAIL_key_value_num = 0
        CANCEL_key_value_num = 0

        # Using for loop 
        for list_pairs in doc_source_key_flag_buckets: 
            print_header("item of list of Elasticsearch hits of aggregations")
            print_header(str(loop_counter))
            print(list_pairs)
            print(type(list_pairs))
            
            # Getting PASS/FAIL/CANCELLED numbers from this element of the list
            element_1st_key_value = list_pairs['key']
            print("1st_key_value: ", element_1st_key_value)

            if element_1st_key_value == pass_status:
                # PASS and FAIL numbers from bucket:    
                PASS_key_item = list_pairs
                print("PASS item: ", PASS_key_item)
                PASS_key_value = PASS_key_item['1']
                print("PASS_key_value: ", PASS_key_value)
                PASS_key_value_num = int(PASS_key_value['value'])
                print("PASS_key_value = %d" % PASS_key_value_num)    
            elif element_1st_key_value == fail_status:
                FAIL_key_item = list_pairs
                print("FAIL item: ", FAIL_key_item)
                FAIL_key_value = FAIL_key_item['1']
                print("FAIL_key_value: ", FAIL_key_value)
                FAIL_key_value_num = int(FAIL_key_value['value'])
                print("FAIL_key_value = %d" % FAIL_key_value_num)
            elif element_1st_key_value == cancel_status:
                CANCEL_key_item = list_pairs
                print("CANCEL item: ", CANCEL_key_item)
                CANCEL_key_value = CANCEL_key_item['1']
                print("CANCEL_key_value: ", CANCEL_key_value)
                CANCEL_key_value_num = int(CANCEL_key_value['value'])
                print("CANCEL_key_value = %d" % CANCEL_key_value_num)
                
            loop_counter = loop_counter + 1

        total_functional_test_records = PASS_key_value_num + FAIL_key_value_num + CANCEL_key_value_num
        print("total count of documents of functional smoke test cases = ", total_functional_test_records)

        func_testcase_PASS_percent = (PASS_key_value_num / total_functional_test_records) * 100
        func_testcase_FAIL_percent = (FAIL_key_value_num / total_functional_test_records) * 100

        print_header( "**** Functional smoke results ****" )
        print("testcase_PASS_percent = ", func_testcase_PASS_percent)
        print("testcase_FAIL_percent = ", func_testcase_FAIL_percent)


    ####################### Performance smoke section #######################

    # Querying the Performance 1/0 rates from performance smoke tests for the EXECUTIVE DASHBOARD
    # at the Vertical level over tests ran today
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services
    ########################################## 
    # counting how many documents belong to testcases for the Government application
    print_header( "fields.product.vertical: Business_Services" )

    vertical_name = "Business_Services"

    filter_query = {
    "aggs": {
        "2": {
          "terms": {
            "field": "fields.product.vertical",
            "order": {
              "_key": "asc"
            },
            "size": 3
          },
          "aggs": {
            "3": {
              "terms": {
                "field": "testcase.anomaly_flag",
                "order": {
                  "_key": "desc"
                },
                "size": 3
              },
              "aggs": {
                "1": {
                  "cardinality": {
                    "field": "product.query.keyword"
                  }
                }
              }
            }
          }
        }
      },
      "size": 0,
      "stored_fields": [
        "*"
      ],
      "script_fields": {},
      "docvalue_fields": [
        {
          "field": "@timestamp",
          "format": "date_time"
        },
        {
          "field": "event.created",
          "format": "date_time"
        }
      ],
      "_source": {
        "excludes": []
      },
      "query": {
        "bool": {
          "must": [],
          "filter": [
            {
              "match_all": {}
            },
            {
              "match_phrase": {
                "fields.Test.environment": "DR"
              }
            },
            {
              "match_phrase": {
                "fields.Test.type": "performance_smoke"
              }
            },
            {
              "match_phrase": {
                "fields.product.vertical": "Business_Services"
              }
            },
            {
              "range": {
                "@timestamp": {
                  "gte": "now-24h/d",
                  "lte": "now/d",
                  "format": "strict_date_optional_time"
                }
              }
            }
          ],
          "should": [],
          "must_not": []
        }
      }
    }

    response = elastic_client.search(index = ES_INDEX, body = filter_query, size = 10)

    print("Got %d Hits:" % response['hits']['total']['value'])

    #Examining the response object
    #print("Got %d Hits:" % response['hits']['total']['value'])
    #print("Got %s Hit relation:" % response['hits']['total']['relation'])

    #Storing the number of hits for looping through list
    number_hits = int(response['hits']['total']['value'])
    print("Number of hits = %d" % number_hits)

    perf_testcase_PASS_percent = 0

    if number_hits > 0:
        #Examining the aggregations object
        print_header("'fields.product.vertical': 'Business_Services'")

        #Since the response hits is a list type of object
        list_aggregations_index_identifier = response['aggregations']['2']
        #print(list_aggregations_index_identifier)
        #print(type(list_aggregations_index_identifier))
        
        #Examining the list_aggregations_index_identifier list object and extracting its necessary elements
        #print_header("First element of list of Elasticsearch hits")
        #print(list_aggregations_index_identifier['buckets'])
        #print(type(list_aggregations_index_identifier['buckets']))

        #print_header("buckets key and its nested key/value pairs:")
        buckets_document_list = list_aggregations_index_identifier['buckets']
        #print(buckets_document_list)

        #print_header("First element of buckets_document_list contains bucket dictionary")
        doc_source_key = buckets_document_list[0]
        #for key, value in doc_source_key.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key[key]))

        #Extracting the "buckets" bucket from inside buckets_document_list since that has PASS/FAIL numbers
        #print_header("Third element [3] of buckets_document_list contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key['3']
        #for key, value in doc_source_key_flag_buckets.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key_flag_buckets[key]))

        #Extracting the "buckets" bucket from inside doc_source_key_flag_buckets since that has PASS/FAIL numbers
        print_header("doc_source_key_flag_buckets contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key_flag_buckets['buckets']
        print(doc_source_key_flag_buckets)
        print(type(doc_source_key_flag_buckets))

        loop_counter = 0

        for item in doc_source_key_flag_buckets:
            print(item)
            print(type(item))
            loop_counter = loop_counter + 1
            
        print("\nsize of doc_source_key_flag_buckets list = %d" % len(doc_source_key_flag_buckets))   

        # Looping through the elements of doc_source_key_flag_buckets to get the PASS and FAIL numbers from the buckets

        # general purpose loop counter
        loop_counter = 1

        # Initializing all PASS/FAIL/CANCELLED variables to 0
        PASS_key_value_num = 0
        FAIL_key_value_num = 0
        CANCEL_key_value_num = 0

        # Using for loop 
        for list_pairs in doc_source_key_flag_buckets: 
            print_header("item of list of Elasticsearch hits of aggregations")
            print_header(str(loop_counter))
            print(list_pairs)
            print(type(list_pairs))
            
            # Getting PASS/FAIL/CANCELLED numbers from this element of the list
            element_1st_key_value = list_pairs['key']
            print("1st_key_value: ", element_1st_key_value)

            if element_1st_key_value == 0:
                # PASS and FAIL numbers from bucket:    
                PASS_key_item = list_pairs
                print("PASS item: ", PASS_key_item)
                PASS_key_value = PASS_key_item['1']
                print("PASS_key_value: ", PASS_key_value)
                PASS_key_value_num = int(PASS_key_value['value'])
                print("PASS_key_value = %d" % PASS_key_value_num)    
            elif element_1st_key_value == 1:
                FAIL_key_item = list_pairs
                print("FAIL item: ", FAIL_key_item)
                FAIL_key_value = FAIL_key_item['1']
                print("FAIL_key_value: ", FAIL_key_value)
                FAIL_key_value_num = int(FAIL_key_value['value'])
                print("FAIL_key_value = %d" % FAIL_key_value_num)
                
            loop_counter = loop_counter + 1

        total_performance_test_records = PASS_key_value_num + FAIL_key_value_num + CANCEL_key_value_num
        print("total count of documents of functional smoke test cases = ", total_performance_test_records)

        perf_testcase_PASS_percent = (PASS_key_value_num / total_performance_test_records) * 100
        perf_testcase_FAIL_percent = (FAIL_key_value_num / total_performance_test_records) * 100

        print_header( "**** Performance smoke results ****" )
        print("testcase_PASS_percent = ", perf_testcase_PASS_percent)
        print("testcase_FAIL_percent = ", perf_testcase_FAIL_percent)


    ####################### Functional smoke section for DataQA #######################

    # Querying the DataQA Functional PASS/FAIL rates from functional dataset contrast between Production and DR tests for the EXECUTIVE DASHBOARD
    # at the Vertical level over tests ran today
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: PublicRecords
    ########################################## 
    # counting how many documents belong to testcases for the application
    print_header( "fields.product.vertical: PublicRecords" )

    vertical_name = "PublicRecords"

    filter_query = {
    "aggs": {
        "2": {
        "terms": {
            "field": "fields.product.vertical",
            "order": {
            "1": "desc"
            },
            "size": 10
        },
        "aggs": {
            "1": {
            "cardinality": {
                "field": "testcase.name.keyword"
            }
            },
            "3": {
            "terms": {
                "field": "testcase.status.keyword",
                "order": {
                "1": "desc"
                },
                "size": 3
            },
            "aggs": {
                "1": {
                "cardinality": {
                    "field": "testcase.name.keyword"
                }
                }
            }
            }
        }
        }
    },
    "size": 0,
    "fields": [
        {
        "field": "@timestamp",
        "format": "date_time"
        }
    ],
    "script_fields": {},
    "stored_fields": [
        "*"
    ],
    "_source": {
        "excludes": []
    },
    "query": {
        "bool": {
        "must": [],
        "filter": [
            {
            "match_all": {}
            },
            {
            "bool": {
                "minimum_should_match": 1,
                "should": [
                {
                    "match_phrase": {
                    "fields.product.vertical": "PublicRecords"
                    }
                },
                {
                    "match_phrase": {
                    "fields.product.vertical": "Insurance"
                    }
                },
                {
                    "match_phrase": {
                    "fields.product.vertical": "Healthcare"
                    }
                }
                ]
            }
            },
            {
            "match_phrase": {
                "fields.product.vertical": "PublicRecords"
            }
            },
            {
            "range": {
                "@timestamp": {
                "gte": "now-24h",
                "lte": "now",
                "format": "strict_date_optional_time"
                }
            }
            }
        ],
        "should": [],
        "must_not": []
        }
    }
    }

    response = elastic_client.search(index = ES_INDEX, body = filter_query, size = 10)

    print("Got %d Hits:" % response['hits']['total']['value'])

    #Examining the response object
    #print("Got %d Hits:" % response['hits']['total']['value'])
    #print("Got %s Hit relation:" % response['hits']['total']['relation'])

    #Storing the number of hits for looping through list
    number_hits = int(response['hits']['total']['value'])
    print("Number of hits = %d" % number_hits)

    DataQA_PublicRecords_func_testcase_PASS_percent = 0

    if number_hits > 0:
        #Examining the aggregations object
        print_header("'fields.product.vertical': 'PublicRecords'")

        #Since the response hits is a list type of object
        list_aggregations_index_identifier = response['aggregations']['2']
        #print(list_aggregations_index_identifier)
        #print(type(list_aggregations_index_identifier))

        #Examining the list_aggregations_index_identifier list object and extracting its necessary elements
        #print_header("First element of list of Elasticsearch hits")
        #print(list_aggregations_index_identifier['buckets'])
        #print(type(list_aggregations_index_identifier['buckets']))

        #print_header("buckets key and its nested key/value pairs:")
        buckets_document_list = list_aggregations_index_identifier['buckets']
        #print(buckets_document_list)

        #print_header("First element of buckets_document_list contains bucket dictionary")
        doc_source_key = buckets_document_list[0]
        #for key, value in doc_source_key.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key[key]))

        #Extracting the "buckets" bucket from inside buckets_document_list since that has PASS/FAIL numbers
        #print_header("Third element [3] of buckets_document_list contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key['3']
        #for key, value in doc_source_key_flag_buckets.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key_flag_buckets[key]))

        #Extracting the "buckets" bucket from inside doc_source_key_flag_buckets since that has PASS/FAIL numbers
        print_header("doc_source_key_flag_buckets contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key_flag_buckets['buckets']
        print(doc_source_key_flag_buckets)
        print(type(doc_source_key_flag_buckets))

        loop_counter = 0

        for item in doc_source_key_flag_buckets:
            print(item)
            print(type(item))
            loop_counter = loop_counter + 1
            
        print("\nsize of doc_source_key_flag_buckets list = %d" % len(doc_source_key_flag_buckets))   

        # Looping through the elements of doc_source_key_flag_buckets to get the PASS and FAIL numbers from the buckets

        # general purpose loop counter
        loop_counter = 1

        # Initializing all PASS/FAIL/CANCELLED variables to 0
        PASS_key_value_num = 0
        FAIL_key_value_num = 0
        CANCEL_key_value_num = 0

        # Using for loop 
        for list_pairs in doc_source_key_flag_buckets: 
            print_header("item of list of Elasticsearch hits of aggregations")
            print_header(str(loop_counter))
            print(list_pairs)
            print(type(list_pairs))
            
            # Getting PASS/FAIL/CANCELLED numbers from this element of the list
            element_1st_key_value = list_pairs['key']
            print("1st_key_value: ", element_1st_key_value)

            if element_1st_key_value == pass_status:
                # PASS and FAIL numbers from bucket:    
                PASS_key_item = list_pairs
                print("PASS item: ", PASS_key_item)
                PASS_key_value = PASS_key_item['1']
                print("PASS_key_value: ", PASS_key_value)
                PASS_key_value_num = int(PASS_key_value['value'])
                print("PASS_key_value = %d" % PASS_key_value_num)    
            elif element_1st_key_value == fail_status:
                FAIL_key_item = list_pairs
                print("FAIL item: ", FAIL_key_item)
                FAIL_key_value = FAIL_key_item['1']
                print("FAIL_key_value: ", FAIL_key_value)
                FAIL_key_value_num = int(FAIL_key_value['value'])
                print("FAIL_key_value = %d" % FAIL_key_value_num)
            elif element_1st_key_value == skip_status:
                CANCEL_key_item = list_pairs
                print("SKIP item: ", CANCEL_key_item)
                CANCEL_key_value = CANCEL_key_item['1']
                print("SKIP_key_value: ", CANCEL_key_value)
                CANCEL_key_value_num = int(CANCEL_key_value['value'])
                print("SKIP_key_value = %d" % CANCEL_key_value_num)
                
            loop_counter = loop_counter + 1

        total_functional_test_records = PASS_key_value_num + FAIL_key_value_num + CANCEL_key_value_num
        print("total count of documents of functional smoke test cases = ", total_functional_test_records)

        DataQA_PublicRecords_func_testcase_PASS_percent = (PASS_key_value_num / total_functional_test_records) * 100
        DataQA_func_testcase_FAIL_percent = (FAIL_key_value_num / total_functional_test_records) * 100

        #Since SKIP essentially means not a FAILure
        DataQA_PublicRecords_func_testcase_PASS_percent = 100 - DataQA_func_testcase_FAIL_percent

        print_header( "**** Functional smoke results DataQA PublicRecords ****" )
        print("testcase_PASS_percent = ", DataQA_PublicRecords_func_testcase_PASS_percent)
        print("testcase_FAIL_percent = ", DataQA_func_testcase_FAIL_percent)


    ####################### Functional regression smoke section #######################

    # Querying the Functional PASS/FAIL rates from functional regression smoke tests for the EXECUTIVE DASHBOARD
    # at the Vertical level over tests ran today
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services
    ########################################## 
    # counting how many documents belong to testcases for the Government application
    print_header( "fields.product.vertical: Business_Services" )

    vertical_name = "Business_Services"

    filter_query = {
        "aggs": {
            "2": {
            "terms": {
                "field": "fields.product.vertical",
                "order": {
                "1": "desc"
                },
                "size": 10
            },
            "aggs": {
                "1": {
                "cardinality": {
                    "field": "testcase.name.keyword"
                }
                },
                "3": {
                "terms": {
                    "field": "testcase.status.keyword",
                    "order": {
                    "1": "desc"
                    },
                    "size": 3
                },
                "aggs": {
                    "1": {
                    "cardinality": {
                        "field": "testcase.name.keyword"
                    }
                    }
                }
                }
            }
            }
        },
        "size": 0,
        "stored_fields": [
            "*"
        ],
        "script_fields": {},
        "docvalue_fields": [
            {
            "field": "@timestamp",
            "format": "date_time"
            },
            {
            "field": "event.created",
            "format": "date_time"
            }
        ],
        "_source": {
            "excludes": []
        },
        "query": {
            "bool": {
            "must": [],
            "filter": [
                {
                "match_all": {}
                },
                {
                "match_phrase": {
                    "fields.Test.environment": "DR"
                }
                },
                {
                "match_phrase": {
                    "fields.product.vertical": "Business_Services"
                }
                },
                {
                "match_phrase": {
                    "fields.Test.type": "functional_regression"
                }
                },
                {
                "range": {
                    "@timestamp": {
                    "gte": "now-24h/d",
                    "lte": "now/d",
                    "format": "strict_date_optional_time"
                    }
                }
                }
            ],
            "should": [],
            "must_not": []
            }
        }
    }

    response = elastic_client.search(index = ES_INDEX, body = filter_query, size = 10)

    print("Got %d Hits:" % response['hits']['total']['value'])

    #Examining the response object
    #print("Got %d Hits:" % response['hits']['total']['value'])
    #print("Got %s Hit relation:" % response['hits']['total']['relation'])

    #Storing the number of hits for looping through list
    number_hits = int(response['hits']['total']['value'])
    print("Number of hits = %d" % number_hits)

    func_regr_testcase_PASS_percent = 0

    if number_hits > 0:
        #Examining the aggregations object
        print_header("'fields.product.vertical': 'Business_Services'")

        #Since the response hits is a list type of object
        list_aggregations_index_identifier = response['aggregations']['2']
        #print(list_aggregations_index_identifier)
        #print(type(list_aggregations_index_identifier))
        
        #Examining the list_aggregations_index_identifier list object and extracting its necessary elements
        #print_header("First element of list of Elasticsearch hits")
        #print(list_aggregations_index_identifier['buckets'])
        #print(type(list_aggregations_index_identifier['buckets']))

        #print_header("buckets key and its nested key/value pairs:")
        buckets_document_list = list_aggregations_index_identifier['buckets']
        #print(buckets_document_list)

        #print_header("First element of buckets_document_list contains bucket dictionary")
        doc_source_key = buckets_document_list[0]
        #for key, value in doc_source_key.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key[key]))

        #Extracting the "buckets" bucket from inside buckets_document_list since that has PASS/FAIL numbers
        #print_header("Third element [3] of buckets_document_list contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key['3']
        #for key, value in doc_source_key_flag_buckets.items() :
        #    print("the key name is " + str(key) + " and its value is " + str(doc_source_key_flag_buckets[key]))

        #Extracting the "buckets" bucket from inside doc_source_key_flag_buckets since that has PASS/FAIL numbers
        print_header("doc_source_key_flag_buckets contains the internal bucket dictionary")
        doc_source_key_flag_buckets = doc_source_key_flag_buckets['buckets']
        print(doc_source_key_flag_buckets)
        print(type(doc_source_key_flag_buckets))

        loop_counter = 0

        for item in doc_source_key_flag_buckets:
            print(item)
            print(type(item))
            loop_counter = loop_counter + 1
            
        print("\nsize of doc_source_key_flag_buckets list = %d" % len(doc_source_key_flag_buckets))   

        # Looping through the elements of doc_source_key_flag_buckets to get the PASS and FAIL numbers from the buckets

        # general purpose loop counter
        loop_counter = 1

        # Initializing all PASS/FAIL/CANCELLED variables to 0
        PASS_key_value_num = 0
        FAIL_key_value_num = 0
        CANCEL_key_value_num = 0

        # Using for loop 
        for list_pairs in doc_source_key_flag_buckets: 
            print_header("item of list of Elasticsearch hits of aggregations")
            print_header(str(loop_counter))
            print(list_pairs)
            print(type(list_pairs))
            
            # Getting PASS/FAIL/CANCELLED numbers from this element of the list
            element_1st_key_value = list_pairs['key']
            print("1st_key_value: ", element_1st_key_value)

            if element_1st_key_value == pass_status:
                # PASS and FAIL numbers from bucket:    
                PASS_key_item = list_pairs
                print("PASS item: ", PASS_key_item)
                PASS_key_value = PASS_key_item['1']
                print("PASS_key_value: ", PASS_key_value)
                PASS_key_value_num = int(PASS_key_value['value'])
                print("PASS_key_value = %d" % PASS_key_value_num)    
            elif element_1st_key_value == fail_status:
                FAIL_key_item = list_pairs
                print("FAIL item: ", FAIL_key_item)
                FAIL_key_value = FAIL_key_item['1']
                print("FAIL_key_value: ", FAIL_key_value)
                FAIL_key_value_num = int(FAIL_key_value['value'])
                print("FAIL_key_value = %d" % FAIL_key_value_num)
            elif element_1st_key_value == cancel_status:
                CANCEL_key_item = list_pairs
                print("CANCEL item: ", CANCEL_key_item)
                CANCEL_key_value = CANCEL_key_item['1']
                print("CANCEL_key_value: ", CANCEL_key_value)
                CANCEL_key_value_num = int(CANCEL_key_value['value'])
                print("CANCEL_key_value = %d" % CANCEL_key_value_num)
                
            loop_counter = loop_counter + 1

        total_functional_regression_test_records = PASS_key_value_num + FAIL_key_value_num + CANCEL_key_value_num
        print("total count of documents of functional regression smoke test cases = ", total_functional_regression_test_records)

        func_regr_testcase_PASS_percent = (PASS_key_value_num / total_functional_regression_test_records) * 100
        func_regr_testcase_FAIL_percent = (FAIL_key_value_num / total_functional_regression_test_records) * 100

        print_header( "**** Functional regression smoke results ****" )
        print("testcase_PASS_percent = ", func_regr_testcase_PASS_percent)
        print("testcase_FAIL_percent = ", func_regr_testcase_FAIL_percent)


    # Storing the PASS % from functional, performance and regression results into Pandas object
    import pandas as pd
    from datetime import date

    today = date.today()
    print("Today's date:", today)
    
    if func_testcase_PASS_percent == 0:
        func_testcase_PASS_percent = np.nan
    if perf_testcase_PASS_percent == 0:
        perf_testcase_PASS_percent = np.nan
    if func_regr_testcase_PASS_percent == 0:
        func_regr_testcase_PASS_percent = np.nan
    if DataQA_PublicRecords_func_testcase_PASS_percent == 0:
        DataQA_PublicRecords_func_testcase_PASS_percent = np.nan

    func_perf_regr_PASS_results_df = pd.DataFrame({"date": [today], "func_testcase_PASS_percent": [func_testcase_PASS_percent], "perf_testcase_PASS_percent": [perf_testcase_PASS_percent], "func_regr_testcase_PASS_percent": [func_regr_testcase_PASS_percent], "DataQA_PublicRecords_testcase_PASS_percent": [DataQA_PublicRecords_func_testcase_PASS_percent]})
    
    print("\n\n")
    print_header("PASS results: ")
    print(func_perf_regr_PASS_results_df)

    import yaml
    from sqlalchemy import create_engine
#    with open('C:\\Users\\KapoSi01\\Documents\\Projects\\Untitled Folder\\config_dr.yaml') as stream:
    with open('/home/Performance_System_analysis/config_dr.yaml') as stream:
        config_details = yaml.load(stream)
        mysql_id = config_details["sqldb"]["username"]
        mysql_password = config_details["sqldb"]["password"]
        mysql_server = config_details["sqldb"]["server"]
        mysql_database = config_details["sqldb"]["database"]
        mysql_port = config_details["sqldb"]["port"]
    
    conn_string = "mysql+pymysql://" + mysql_id + ":" + mysql_password + "@" + mysql_server + ":" + mysql_port + "/" + mysql_database
    
    from sqlalchemy.pool import NullPool
    engine = create_engine(conn_string, poolclass = NullPool)

    conn = engine.connect()
    func_perf_regr_PASS_results_df.to_sql("dr_elk", conn, if_exists="append", index=False, chunksize=171,
                                          method="multi")
    print(func_perf_regr_PASS_results_df)
    conn.close()
    engine.dispose()


def main_elk(ds, **kwargs):
    # Invoking the anomaly_detection_function big function (will be broken down into constituent functions later)
    ########################################## 
    #/***
    # *                      .___       
    # *      ____   ____   __| _/ ____  
    # *    _/ ___\ /  _ \ / __ |_/ __ \ 
    # *    \  \___(  <_> ) /_/ |\  ___/ 
    # *     \___  >\____/\____ | \___  >
    # *         \/            \/     \/ 
    # */
    # Vertical: Business_Services 
    ########################################## 
    from datetime import date,timedelta
    from sqlalchemy import create_engine
    import pandas as pd
#    today = date.today()
    today = datetime.now(tz)
    new_date = today - timedelta(days=1)
    today = today.strftime('%Y-%m-%d')
    new_date = new_date.strftime('%Y-%m-%d')
#    with open('C:\\Users\\KapoSi01\\Documents\\Projects\\Untitled Folder\\config_dr.yaml') as stream:
    with open('/home/Performance_System_analysis/config_dr.yaml') as stream:
        config_details = yaml.load(stream)
        mysql_id = config_details["sqldb"]["username"]
        mysql_password = config_details["sqldb"]["password"]
        mysql_server = config_details["sqldb"]["server"]
        mysql_database = config_details["sqldb"]["database"]
        mysql_port = config_details["sqldb"]["port"]

    conn_string = "mysql+pymysql://" + mysql_id + ":" + mysql_password + "@" + mysql_server + ":" + mysql_port + "/" + mysql_database
    engine = create_engine(conn_string)
    conn = engine.connect()

    query = "Select * from dr_elk where date = '"+ str(today) +"';"
    df = pd.read_sql(query,conn)

    # Closing the YAML stream for possible further reuse:
    print(stream)
    stream.close()

    if df.empty:
        conn.close()
        engine.dispose()
        func_perf_regr_smoke_data_collection_function()
    else:
        string = "Delete from dr_elk where Date = '" + str(today) + "';"
        qry = text(string)
        conn.execute(qry)
        conn.close()
        engine.dispose()
        func_perf_regr_smoke_data_collection_function()
        print("data already in database")
    

dag = DAG(dag_id='DR_ELK', default_args=default_args,schedule_interval='0 11-19 * * *')

start_get_elk = PythonOperator(
    task_id='t_main_elk', trigger_rule = 'all_done',
    provide_context=True,
    python_callable=main_elk,
    dag=dag)

start_task = DummyOperator(task_id = 'start',  trigger_rule = 'all_done', dag = dag)
end_task = DummyOperator(task_id = 'end',  trigger_rule = 'all_done', dag = dag)

start_task.set_downstream(start_get_elk)
start_get_elk.set_downstream(end_task)