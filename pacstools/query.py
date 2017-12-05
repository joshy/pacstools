import logging
import os

import luigi
import pandas as pd
from luigi.contrib.external_program import ExternalProgramTask
from pydicom.dataset import Dataset
from pynetdicom3 import AE, QueryRetrieveSOPClassList


def create_dataset(accession_number, study_uid):
    dataset = Dataset()
    dataset.QueryRetrieveLevel = 'SERIES'
    dataset.AcccessionNumber = accession_number
    dataset.StudyInstanceUID = study_uid
    dataset.SeriesInstanceUID = ''
    return dataset


def search(assoc, dataset):
    responses = assoc.send_c_find(dataset, query_model='S')
    for (status, dataset) in responses:
        if dataset:
            yield dataset.StudyInstanceUID, dataset.SeriesInstanceUID


class QueryTask(luigi.Task):
    accession_number = luigi.Parameter()
    study_uid = luigi.Parameter()
    aet = luigi.Parameter()
    aec = luigi.Parameter()
    peer_address = luigi.Parameter()
    peer_port = luigi.IntParameter()
    incoming_port = luigi.IntParameter()

    def run(self):
        ae = AE(ae_title=self.aet,
            port=self.incoming_port,
            scu_sop_class=QueryRetrieveSOPClassList)

        ('Requesting Association with the peer')
        assoc = ae.associate(addr=self.peer_address,
                             port=self.peer_port,
                             ae_title=self.aec)
        if assoc.is_established:
            logging.debug('Association accepted by the peer')
            dataset = create_dataset(self.accession_number, self.study_uid)
            with self.output().open('w') as out:
                out.write('StudyInstanceUID,SeriesInstanceUID\n')
                for s_uid, series_uid in [x for x in search(assoc, dataset)]:
                    out.write('{},{}\n'.format(s_uid, series_uid))
        assoc.release()

    def output(self):
        p = os.path.join('workdir', str(self.accession_number) + '.csv')
        return luigi.LocalTarget(p)


class QueryAllTask(luigi.Task):

    file = luigi.Parameter()

    def run(self):
        csv = pd.read_csv(self.file)
        for i, r in csv.iterrows():
            yield QueryTask(accession_number=r['AccessionNumber'],
                            study_uid=r['StudyInstanceUID'])


if __name__ == '__main__':
    luigi.run()
