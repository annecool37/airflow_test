
import pandas as pd
import click
import os

from datetime import datetime

@click.command()
@click.option('--fn', default = 'demo')
@click.option('--task')
def main(**kwargs):
    '''
    '''

    task = kwargs['task']
    fn = kwargs['fn']

    if task == 'write_csv':
        df = pd.DataFrame([1, 2, 3])
        now = datetime.now()
        path = os.path.join('files', fn + str(now) + '.csv')
        df.to_csv(path, index = False)

    else:
        print ('blah blah blah')


if __name__ == '__main__':
    main()

