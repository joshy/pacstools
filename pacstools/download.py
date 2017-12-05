import os
import logging
from os.path import splitext, basename

import luigi
import pandas as pd
from luigi.contrib.external_program import ExternalProgramTask



class DownloadTask(luigi.contrib.external_program.ExternalProgramTask):
    study_uid = luigi.Parameter()
    accession_number = luigi.Parameter()
    aet = luigi.Parameter()
    aec = luigi.Parameter()
    peer_address = luigi.Parameter()
    peer_port = luigi.IntParameter()
    incoming_port = luigi.IntParameter()
    dcmin = luigi.Parameter()
    movescu = luigi.Parameter()
    output_dir = luigi.Parameter()


    def program_args(self):
        out = os.path.join(self.output_dir, self.accession_number)
        if not os.path.exists(out):
            os.makedirs(out, exist_ok=True)
        study_uid = 'StudyInstanceUID={}'.format(self.study_uid)
        command = [self.movescu, '--verbose', '--study',
             '--aetitle', self.aet,
             '--call', self.aec, self.peer_address, self.peer_port,
             '--port', self.incoming_port,
             '-k', 'QueryRetrieveLevel=SERIES',
             '-k', study_uid,
             '--output-directory', out, self.dcmin
        ]
        return command


    def run(self):
        super().run()
        with self.output().open('w') as output:
            output.write('Done')

    def output(self):
        name = str(self.accession_number).rstrip() + '.run_success'
        out = os.path.join('dicoms', name)
        return luigi.LocalTarget(out)


class DownloadStudyTask(luigi.Task):

    file = luigi.Parameter()

    def run(self):
        csv = pd.read_csv(self.file)
        for i, row in csv.iterrows():
            yield DownloadTask(study_uid=row['StudyInstanceUID'],
                         accession_number=str(row['AccessionNumber']))


if __name__ == '__main__':
    luigi.run()