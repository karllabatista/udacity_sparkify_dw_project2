import boto3
import configparser
from cluster import Cluster             

class ClusterManager:
    
    """Manager deploy of cluster
    
    """

    def __init__(self):
        """
        Parameters:
        ----------
        None
        """
        
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))
        self.KEY     = config.get('AWS','KEY')
        self.SECRET  = config.get('AWS','SECRET')
    
    def setup_resources(self):
        """Initialize AWS services and deploy the Redshift cluster.
    
        This method sets up connections to EC2, S3, IAM, and Redshift services,
        and then uses these services to deploy the Redshift cluster.

        Paramenters
        ---------
        None

        Returns
        ------
        None
        """
        
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
        cluster.deploy_cluster(redshift,iam,ec2)
          
       
if __name__=='__main__':
        
    cm=ClusterManager()
    cm.setup_resources()
 
    
            
