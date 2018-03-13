from rq import Queue
from redis import Redis
import pandas as pd
import click

from pacstools.job import program_args


def fill(file):
    redis_con = Redis()
    q = Queue(connection=redis_con)
    df = pd.read_csv(file)
    df = df[20:101]
    for row in df.itertuples():
        q.enqueue(
            program_args,
            timeout='10m',
            patient_id=row.PatientID,
            accession_number=str(row.AccessionNumber),
            study_instance_uid=row.StudyInstanceUID,
            output_dir='/data/projects/federau')


@click.command()
@click.argument('file')
def run(file):
    fill(file)

if __name__ == '__main__':
    run()