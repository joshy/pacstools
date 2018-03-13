import configparser
import os

import subprocess
from rq import Queue
from redis import Redis



def program_args(patient_id, accession_number, study_instance_uid, output_dir):
    out = os.path.join(output_dir, patient_id, accession_number)
    config = read_config()
    if not os.path.exists(out):
        os.makedirs(out, exist_ok=True)

    patient = 'PatientID={}'.format(patient_id)
    study_uid = 'StudyInstanceUID={}'.format(study_instance_uid)
    command = [config['movescu'],
            '--aetitle', config['aet'],
            '--call', config['aec'], config['peer_address'], config['peer_port'],
            '--port', config['incoming_port'],
            '-k', 'QueryRetrieveLevel=STUDY',
            '-k', patient,
            '-k', study_uid,
            '--output-directory', out, config['dcmin']
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False)
    result.check_returncode()
    return 'Success'


def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['Download']
