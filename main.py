import ast
import logging
import usaddress
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

#logging.basicConfig(filename='Pipeline_to_read_pub_sub.log', level=logging.DEBUG)


# method to convert byte to key,value pair.
class PrintValue(beam.DoFn):
    def process(self, element):
        try:
            element = element.decode("utf-8")
            decoded_value = ast.literal_eval(element)
            # print(decoded_value)
            return [decoded_value]
        except:
            logging.exception('Error at print value function')


# pardo function to split customer Name
class Customer_name_change(beam.DoFn):
    def process(self, element):
        try:
            if 'customer_name' in element:
                result_name = element['customer_name'].split(" ")

                element['customer_first_name'] = result_name[0]
                element['customer_last_name'] = result_name[1]
            # print(element)
            return [element]
        except:
            logging.exception('Error at customer name change function')


# Splitting address
class splitting_order_address(beam.DoFn):

    def process(self, dict_input):
        list_value = {}
        try:

            # print(type(dict_input))
            if 'order_address' in dict_input:

                data_address = usaddress.tag(dict_input['order_address'])
                if "AddressNumber" in data_address[0].keys():
                    list_value['order_building_number'] = int(data_address[0]["AddressNumber"])
                list_value['order_street_name'] = ""
                if "AddressNumberPrefix" in data_address[0].keys():
                    list_value['order_street_name'] = data_address[0]["AddressNumberPrefix"]

                if "AddressNumberSuffix" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["AddressNumberSuffix"]
                if "USPSBoxGroupID" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["USPSBoxGroupID"]
                if "USPSBoxGroupType" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["USPSBoxGroupType"]
                if "USPSBoxID" in data_address[0].keys():
                    for k in data_address[0]["USPSBoxID"]:
                        list_value['order_street_name'] += " " + k
                if "USPSBoxType" in data_address[0].keys():
                    if data_address[0]["USPSBoxType"] in "Box":
                        list_value['order_street_name'] += " " + "Box"
                    elif data_address[0]["USPSBoxType"] in "PSC":
                        list_value['order_street_name'] += " " + "PSC"

                if "BuildingName" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["BuildingName"]
                if "CornerOf" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["CornerOf"]
                if "IntersectionSeparator" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["IntersectionSeparator"]
                if "LandMarkName" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["LandMarkName"]
                if "OccupancyType" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["OccupancyType"]
                if "OccupancyIdentifier" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["OccupancyIdentifier"]

                if "StreetName" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetName"]
                if "StreetNamePreType" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePreType"]
                if "StreetNamePostType" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePostType"]
                if "StreetNamePreDirectional" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePreDirectional"]
                if "StreetNamePostDirectional" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePostDirectional"]

                if "StreetNamePreModifier" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePreModifier"]
                if "StreetNamePostModifier" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["StreetNamePostModifier"]

                if "SubaddressIdentifier" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["SubaddressIdentifier"]
                if "SubaddressType" in data_address[0].keys():
                    list_value['order_street_name'] += " " + data_address[0]["SubaddressType"]
                if "PlaceName" in data_address[0].keys():
                    list_value['order_city'] = str(data_address[0]["PlaceName"])

                if "StateName" in data_address[0].keys():
                    list_value['order_state_code'] = str(data_address[0]["StateName"])
                if "ZipCode" in data_address[0].keys():
                    list_value["order_zip_code"] = int(data_address[0]["ZipCode"])
            # print(dict_input)
            # return dict_input
        except usaddress.RepeatedLabelError as e:
            list_value['order_building_number'] = 0
            list_value['order_street_name'] = " "
            list_value['order_city'] = ""
            list_value['order_state_code'] = ""
            list_value["order_Zip_code"] = 0
            logging.exception(e)
        finally:
            # print(type(dict_input))
            dict_input['order_new_address'] = list_value
            # print(dict_input)
            return [dict_input]


class total_cost(beam.DoFn):
    def process(self, dict_input):
        try:
            if 'order_items' in dict_input:
                dict_input['cost_total'] = 0.0
                for i in dict_input['order_items']:
                    dict_input['cost_total'] = dict_input['cost_total'] + float(i['price'])
                dict_input['cost_total'] = dict_input['cost_total'] + float(
                    dict_input['cost_shipping'] + dict_input['cost_tax'])

            # print(dict_input)
            return [dict_input]

        except:
            logging.exception('exception at total cost')
        finally:
            print("success")


table_schema = bigquery.TableSchema()

customer_ip_schema = bigquery.TableFieldSchema()
customer_ip_schema.name = 'customer_ip'
customer_ip_schema.type = 'string'
customer_ip_schema.mode = 'nullable'
table_schema.fields.append(customer_ip_schema)

# Fields that use standard types.
order_id_schema = bigquery.TableFieldSchema()
order_id_schema.name = 'order_id'
order_id_schema.type = 'integer'
order_id_schema.mode = 'nullable'
table_schema.fields.append(order_id_schema)

order_currency_schema = bigquery.TableFieldSchema()
order_currency_schema.name = 'order_currency'
order_currency_schema.type = 'string'
order_currency_schema.mode = 'nullable'
table_schema.fields.append(order_currency_schema)

customer_first_name_schema = bigquery.TableFieldSchema()
customer_first_name_schema.name = 'customer_first_name'
customer_first_name_schema.type = 'string'
customer_first_name_schema.mode = 'nullable'
table_schema.fields.append(customer_first_name_schema)

customer_last_name_schema = bigquery.TableFieldSchema()
customer_last_name_schema.name = 'customer_last_name'
customer_last_name_schema.type = 'string'
customer_last_name_schema.mode = 'nullable'
table_schema.fields.append(customer_last_name_schema)

order_new_address_schema = bigquery.TableFieldSchema()
order_new_address_schema.name = 'order_new_address'
order_new_address_schema.type = 'record'
order_new_address_schema.mode = 'nullable'
table_schema.fields.append(order_new_address_schema)

order_building_number_schema = bigquery.TableFieldSchema()
order_building_number_schema.name = 'order_building_number'
order_building_number_schema.type = 'integer'
order_building_number_schema.mode = 'nullable'
order_new_address_schema.fields.append(order_building_number_schema)

order_street_name_schema = bigquery.TableFieldSchema()
order_street_name_schema.name = 'order_street_name'
order_street_name_schema.type = 'string'
order_street_name_schema.mode = 'nullable'
order_new_address_schema.fields.append(order_street_name_schema)

order_city_schema = bigquery.TableFieldSchema()
order_city_schema.name = 'order_city'
order_city_schema.type = 'string'
order_city_schema.mode = 'nullable'
order_new_address_schema.fields.append(order_city_schema)

order_state_code_schema = bigquery.TableFieldSchema()
order_state_code_schema.name = 'order_state_code'
order_state_code_schema.type = 'string'
order_state_code_schema.mode = 'nullable'
order_new_address_schema.fields.append(order_state_code_schema)

order_zip_code_schema = bigquery.TableFieldSchema()
order_zip_code_schema.name = 'order_zip_code'
order_zip_code_schema.type = 'integer'
order_zip_code_schema.mode = 'nullable'
order_new_address_schema.fields.append(order_zip_code_schema)

cost_total_schema = bigquery.TableFieldSchema()
cost_total_schema.name = 'cost_total'
cost_total_schema.type = 'float'
cost_total_schema.mode = 'nullable'
table_schema.fields.append(cost_total_schema)


# unused
class table_trim(beam.DoFn):
    def process(self, element):
        # print("reached here")
        if 'customer_name' in element:
            element.pop('customer_name')
        if 'customer_time' in element:
            element.pop('customer_time')
        if 'order_currency' in element:
            element.pop('order_currency')
        if 'customer_ip' in element:
            element.pop('customer_ip')
        if 'order_address' in element:
            element.pop('order_address')
        if 'cost_shipping' in element:
            element.pop('cost_shipping')
        if 'cost_tax' in element:
            element.pop('cost_tax')
        encode_dict = (str(element)).encode("utf-8")

        # print (encode_dict)
        return encode_dict


# unused
class table_pop_value(beam.DoFn):
    def process(self, element):
        # print("reached here")
        if 'customer_name' in element and 'customer_first_name' in element:
            element.pop('customer_name')
        if 'customer_time' in element:
            element.pop('customer_time')
        # if 'order_currency' in element:
        #     element.pop('order_currency')

        if 'order_address' in element and 'order_new_address' in element:
            element.pop('order_address')
        if 'cost_shipping' in element and 'cost_total' in element:
            element.pop('cost_shipping')
        if 'cost_tax' in element and 'cost_total' in element:
            element.pop('cost_tax')
        if 'order_items' in element:
            element.pop('order_items')

        # value_new=(str(element)).encode('utf-8')
        # print(element)
        return element


class module(beam.DoFn):
    def process(self, element):
        # print("module")
        list_new = []
        for k in element['order_items']:
            list_new.append(k['id'])
        element['items_list'] = list_new
        #print(list_new)
        columns_to_extract = ['order_id', 'items_list']
        new_set = {k: element[k] for k in columns_to_extract}

        return [new_set]


class module_bigquery(beam.DoFn):
    def process(self, element):
        # print("module")
        columns_to_extract = ['customer_ip', 'order_id', 'order_currency', 'customer_first_name', 'customer_last_name',
                              'order_new_address', 'cost_total']
        new_set = {k: element[k] for k in columns_to_extract}

        return [new_set]


if __name__ == '__main__':
    try:

        pipeline_options = PipelineOptions(
            streaming=True,
            temp_location='gs://bucketforwordcount_jm_york/'
        )

        with beam.Pipeline(options=pipeline_options) as pipeline:
            # try:
            table_spec = bigquery.TableReference(projectId='york-cdf-start', datasetId='jaya_mohan_proj1',
                                                 tableId='usd_order_payment_history')

            table_names = (pipeline | "create input" >> beam.Create(
                [('USD', 'york-cdf-start:jaya_mohan_proj1.usd_order_payment_history'),
                 ('EUR', 'york-cdf-start:jaya_mohan_proj1.eur_order_payment_history'),
                 ('GBP', 'york-cdf-start:jaya_mohan_proj1.gbp_order_payment_history')]))
            table_names_dict = beam.pvalue.AsDict(table_names)

            lines = pipeline | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                subscription="projects/york-cdf-start/subscriptions/dataflow-project-orders-sub") | beam.ParDo(
                PrintValue())
            added_value = lines | beam.ParDo(Customer_name_change()) | beam.ParDo(splitting_order_address())
            final_addition = added_value | beam.ParDo(total_cost())
            final_addition | beam.ParDo(module_bigquery()) | 'write' >> beam.io.WriteToBigQuery(
                table=lambda row, table_dict: table_dict[row['order_currency']],
                table_side_inputs=(table_names_dict,),
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            final_addition | beam.ParDo(module()) | beam.Map(
                lambda text: (str(text)).encode('utf-8')) | beam.io.WriteToPubSub(
                'projects/york-cdf-start/topics/publish_jaya_msg')
            logging.info('Succcess run')
    except:
        logging.exception('Run method failure')








