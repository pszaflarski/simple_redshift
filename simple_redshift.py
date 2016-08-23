import json
import psycopg2
import csv

class redshift_controller():
    def __init__(self):
        self.creds = None

    def load_settings_from_file(self, json_file):
        d = json.load(json_file)
        self.set_creds(d["redshift_creds"])

    def set_creds(self,cred_dict):
        self.creds = cred_dict

    def query(self,query_text):
        conn = psycopg2.connect(**self.creds)
        cur = conn.cursor()
        cur.execute(query_text)
        result = cur.fetchall()
        return result

    def query_to_csv(self,query_text,csvfile, encoding = 'utf-8'):
        result = self.query(query_text)
        with open(csvfile, 'w', encoding=encoding) as csv_out:
            writer = csv.writer(csv_out)
            writer.writerows(result)

    def command(self, command_text):
        conn = psycopg2.connect(**self.creds)
        cur = conn.cursor()
        cur.execute(command_text)
        result = conn.commit()
        return result

if __name__=='__main__':
    pass
