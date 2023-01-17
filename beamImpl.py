import apache_beam as beam


class SplitDataset1(beam.DoFn):
    def process(self, element):
        invoice_id, legal_entity, counter_party, rating, status, value = element.split(',')
        return [{
            'invoice_id': int(invoice_id),
            'legal_entity': str(legal_entity),
            'counter_party': str(counter_party),
            'rating': float(rating),
            'status': str(status),
            'value': float(value),
        }]


class SplitDataset2(beam.DoFn):
    def process(self, element):
        counter_party, tier = element.split(',')
        return [{
            'counter_party': str(counter_party),
            'tier': int(tier),
        }]


# Code taken from https://github.com/HocLengChung/Apache-Beam-Dataflow-for-public/blob/master/leftjoin-blogexample.py
class UnnestCoGrouped(beam.DoFn):
    """This DoFn class unnests the CogroupBykey output and emits """

    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


class CountDistinct(beam.CombineFn):
    def create_accumulator(self):
        distinct = []
        count = 0
        accumulator = distinct, count
        return accumulator

    def add_input(self, accumulator, input):
        distinct, count = accumulator
        if input not in distinct:
            distinct.append(input)
            count += 1
        return distinct, count

    def extract_output(self, accumulator):
        distinct, count = accumulator
        if count == 0:
            return float('NaN')
        return count


with beam.Pipeline() as p:
    dataset1 = (p
                | 'Read from dataset1' >> beam.io.ReadFromText('dataset1.csv', skip_header_lines=1)
                | beam.ParDo(SplitDataset1())
                | 'index ds1 on counter_party' >> beam.Map(lambda row: (row['counter_party'], row)))

    dataset2 = (p
                | 'Read from dataset2' >> beam.io.ReadFromText('dataset2.csv', skip_header_lines=1)
                | beam.ParDo(SplitDataset2())
                | 'index ds2 on counter_party' >> beam.Map(lambda row: (row['counter_party'], row)))

    initialDS = ({'ds1': dataset1, 'ds2': dataset2}
                 | 'left join' >> beam.CoGroupByKey()
                 | 'flatten cogroup' >> beam.ParDo(UnnestCoGrouped(), 'ds1', 'ds2')
                 | beam.Map(lambda x: beam.Row(legal_entity=x['legal_entity'],
                                               counter_party=x['counter_party'],
                                               rating=x['rating'],
                                               status=x['status'],
                                               value=x['value'],
                                               tier=x['tier'])))

    legalDS = (initialDS
               | 'group by legal_entity' >> beam.GroupBy('legal_entity')
               .aggregate_field('counter_party', CountDistinct(), 'counter_party')
               .aggregate_field('tier', CountDistinct(), 'tier')
               .aggregate_field('rating', max, 'max_rating_by_counter_party')
               .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
               .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
               | 'Reformat to row 1' >> beam.Map(lambda x: beam.Row(legal_entity=x[0],
                                                                    counter_party=x[1],
                                                                    tier=x[2],
                                                                    max_rating_by_counter_party=x[3],
                                                                    sum_value_where_status_ARAP=x[4],
                                                                    sum_value_where_status_ACCR=x[5])))

    counterDS = (initialDS
                 | 'group by counter_party' >> beam.GroupBy('counter_party')
                 .aggregate_field('legal_entity', CountDistinct(), 'legal_entity')
                 .aggregate_field('tier', CountDistinct(), 'tier')
                 .aggregate_field('rating', max, 'max_rating_by_counter_party')
                 .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
                 .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
                 | 'Reformat to row 2' >> beam.Map(lambda x: beam.Row(legal_entity=x[1],
                                                                      counter_party=x[0],
                                                                      tier=x[2],
                                                                      max_rating_by_counter_party=x[3],
                                                                      sum_value_where_status_ARAP=x[4],
                                                                      sum_value_where_status_ACCR=x[5])))

    tierDS = (initialDS
              | 'group by tier' >> beam.GroupBy('tier')
              .aggregate_field('legal_entity', CountDistinct(), 'legal_entity')
              .aggregate_field('counter_party', CountDistinct(), 'counter_party')
              .aggregate_field('rating', max, 'max_rating_by_counter_party')
              .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
              .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
              | 'Reformat to row 3' >> beam.Map(lambda x: beam.Row(legal_entity=x[1],
                                                                   counter_party=x[2],
                                                                   tier=x[0],
                                                                   max_rating_by_counter_party=x[3],
                                                                   sum_value_where_status_ARAP=x[4],
                                                                   sum_value_where_status_ACCR=x[5])))

    legalCounterDS = (initialDS
                      | 'group by legal then counter' >> beam.GroupBy('legal_entity', 'counter_party')
                      .aggregate_field('tier', CountDistinct(), 'tier')
                      .aggregate_field('rating', max, 'max_rating_by_counter_party')
                      .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
                      .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
                      | 'Reformat to row 4' >> beam.Map(lambda x: beam.Row(legal_entity=x[0],
                                                                           counter_party=x[1],
                                                                           tier=x[2],
                                                                           max_rating_by_counter_party=x[3],
                                                                           sum_value_where_status_ARAP=x[4],
                                                                           sum_value_where_status_ACCR=x[5])))

    fileHeader = 'legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)'

    finalDS = ((legalDS, counterDS, tierDS, legalCounterDS)
               | beam.Flatten():

               | beam.Map(lambda x: tuple(x))
               | beam.Map(lambda x: ''.join(','.join(str(y) for y in x)))
               | beam.io.WriteToText('output/beamOut.csv', shard_name_template='', header=fileHeader))

