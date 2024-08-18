import boto3
import configparser
from cluster import Cluster             
from create_tables import inicialize_database

class CreateDatabase:

    def __init__(self):
        
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))
        self.KEY     = config.get('AWS','KEY')
        self.SECRET  = config.get('AWS','SECRET')
    
    def run(self):
        
        ec2 = boto3.resource('ec2',
                        region_name='us-west-2', 
                        aws_access_key_id=self.KEY,
                        aws_secret_access_key=self.SECRET)

        s3 = boto3.client('s3',region_name='us-west-2', 
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET)

        iam = boto3.client('iam',region_name='us-west-2', 
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET)

        redshift = boto3.client('redshift',
                                region_name='us-west-2', 
                                aws_access_key_id=self.KEY,
                                aws_secret_access_key=self.SECRET)
        
        cluster = Cluster()
        ready = cluster.deploy_cluster(redshift,iam,ec2)
        
        if ready:

            inicialize_database()
        

          
       
if __name__=='__main__':
        
    create_db =CreateDatabase()
    create_db.run()
 
    
            
