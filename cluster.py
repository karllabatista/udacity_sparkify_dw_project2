import json
import configparser
import pandas as pd
import time

class Cluster:
    """
    A class used to represents a cluster

    ...
    
    """

    def __init__(self):
        """
        Parameters
        ----------
        
        """
        print("CREATING A REDSHIFT CLUSTER.....")

        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))

        self.KEY                    = config.get('AWS','KEY')
        self.SECRET                 = config.get('AWS','SECRET')

        self.HOST                   = config.get("CLUSTER","HOST")
        self.DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB_NAME            = config.get("CLUSTER","DB_NAME")
        self.DWH_DB_USER            = config.get("CLUSTER","DB_USER")
        self.DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
        self.DWH_PORT               = config.get("CLUSTER","DB_PORT")
        self.ENDPOINT               = config.get("CLUSTER","HOST")

        self.DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

        (self.DWH_DB_USER, self.DWH_DB_PASSWORD, self.DWH_DB_NAME)

        pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB_NAME", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [self.DWH_CLUSTER_TYPE, self.DWH_NUM_NODES, self.DWH_NODE_TYPE, self.DWH_CLUSTER_IDENTIFIER, self.DWH_DB_NAME, self.DWH_DB_USER, self.DWH_DB_PASSWORD, self.DWH_PORT, self.DWH_IAM_ROLE_NAME]
             })
        
        self.myClusterProps = {}


        

    def create_iam_role_redshift(self,iam):
        """Create a role to Redshift read S3 objects

        Parameters
        --------
        iam : boto3.client
            A boto3 client instance for AWS Identity and Access Management (IAM).
        
        Returns
        -------
        None
            This function does not return a value.

        """
        try:
            print('Creating a new IAM Role')
            dwhRole = iam.create_role(
            Path='/',
            RoleName=self.DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps
                (
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'}
                ),

            Description='Allows Redshift clusters to call AWS services on your behalf.'
                
        )
            

        except Exception as error:
            print(error)

    def attach_policy(self,iam):
        """Attach a policy to Redshift read S3 objects

        Parameters
        --------
        iam : boto3.client
           A boto3 client instance for AWS Identity and Access Management (IAM).

        """

        iam.attach_role_policy(
        RoleName=self.DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


    def get_ARN(self,iam):
        """ Retrieve the ARN of an IAM role.

        Parameters
        ----------
        iam : boto3.client
            A boto3 client instance for AWS Identity and Access Management (IAM).

        Returns
        -------
        str
            The ARN of the IAM role.

        """
     
        print('Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn'] 
        print(roleArn)

        return roleArn
    
    def set_roles_config(self,iam):
        """
        Configure IAM roles for the service.

        This function performs the following actions:
        - Creates an IAM role for Redshift.
        - Attaches a policy to the IAM role.
        - Retrieves the ARN of the IAM role.

        Parameters
        ----------
        iam : boto3.client
            A boto3 client instance for AWS Identity and Access Management (IAM).

        Returns
        -------
        None
            This function does not return a value.

        """

        self.create_iam_role_redshift(iam)
        self.attach_policy(iam)
        self.get_ARN(iam)



    def create_cluster(self,redshift,roleArn):
        """Configure a new cluster Redshift

        Parameters
        --------

        redshift : boto3.client
            A boto3 client instance for Redshift
        
        roleArn: str
            The ARN of the IAM role to be associated with the Redshift cluster for S3 access.

        Returns
        -------
        None
            This function does not return a value.

        """
        try:
            response = redshift.create_cluster(        
                
                #HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),
                

                #Identifiers & Credentials
                DBName=self.DWH_DB_NAME,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
                
            )
        except Exception as e:
            print(e)
    

    def check_available_cluster(self,redshift,max_attemp=30,wait_interval=30):
        """Check cluster availability

            Check if cluster was created with success
        
        Parameters
        ---------

        redishift : boto3.client
            A boto3 client instance for Redshift

        max_attemp: int, optional
            Number of connect attemps [default is 30]

        wait_interval: int, optional
            The interval, in seconds, to wait between polling or retries
            This variable is used to specify the amount of time to wait before 
            retrying a failed operation or before polling for a status update. 
            [default is 30]
        
        Returns
        -------
        bool
           Status of redshoft cluster
        """
     
        attempt=0
        cluster_status= ""
        cluster_identifier =""

        
        while attempt < max_attemp:

            try:
                self.myClusterProps  = redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
                #print(myClusterProps)
                cluster_status = self.myClusterProps['ClusterStatus']
                cluster_identifier = self.myClusterProps['ClusterIdentifier']
                
                print(f"Cluster {cluster_identifier} status: {cluster_status}")

                if cluster_status == 'available':
                    print(f"Cluster {self.myClusterProps['ClusterIdentifier']} is available.")
                    return True
                else:
                    print(f"Cluster {self.myClusterProps['ClusterIdentifier']} is still creating...")
        
            except Exception as error:
                print(f"Error checking cluster status: {error}")

            # Esperar antes de verificar novamente
            time.sleep(wait_interval)
            attempt += 1

        print(f"Cluster {cluster_identifier} did not become available within the expected time.")
        return False


    def set_host_cluster(self):
        """Storage the  cluster endpoint in the HOST variable
           Open the dwh.cfg file and set the value of HOST with the cluster endpoint ]

        Parameters
        ----------
        None

        Returns
        -------
        None
            This function does not return a value.
        """

        try:
            config = configparser.ConfigParser()
            config.read_file(open('dwh.cfg'))

            config.set("CLUSTER","HOST",self.myClusterProps['Endpoint']['Address'])

            # Write the updated configuration back to the file
            with open("dwh.cfg", 'w') as configfile:
                config.write(configfile)
        except Exception as error:
            print(f"Error to set host of cluster: {error}")


    def access_cluster_by_endpoint(self,ec2):
        """Configures access to a cluster endpoint by modifying security group rules.

        This function checks if a specific inbound rule already exists in the default 
        security group associated with the VPC of the cluster. If the rule does not 
        exist, it adds the rule to allow access from any IP address on the specified port.

        Parameters
        ----------
        ec2 : boto3.resource
            A boto3 resource instance for Amazon EC2. This is used to interact with EC2 
            resources such as VPCs and security groups.

        Returns
        -------
        None
            This function does not return a value.

        Raises
        ------
        Exception
            If there is an error while checking or modifying security group rules, an 
            exception is caught and an error message is printed.

        Notes
        -----
        - The function assumes that the `self.myClusterProps['VpcId']` contains a valid VPC ID.
        - The function also relies on `self.DWH_PORT` being set to the port number to allow 
        in the security group rule.
        - If the rule already exists, a message is printed and no changes are made.
        - If the rule does not exist, it is added to allow TCP traffic on the specified port 
        from any IP address (0.0.0.0/0).

        """
        
        rule_exists = False
        try:
            
            
            vpc = ec2.Vpc(id=self.myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(list(vpc.security_groups.all()))
            print(defaultSg)
            print("------------------------------------")
            print(defaultSg.ip_permissions) 

            # Define os parâmetros da regra
            cidr_ip = '0.0.0.0/0'
            ip_protocol = 'tcp'
            from_port = int(self.DWH_PORT)
            to_port = int(self.DWH_PORT)
            

            existings_permissions = defaultSg.ip_permissions

            for rule in existings_permissions:

                if all(key in rule for key in ['FromPort', 'ToPort', 'IpProtocol']):
                    
                    if (rule['FromPort'] == from_port and rule['ToPort'] == to_port and rule['IpProtocol'] == ip_protocol):
                            
                        for ip_range in rule['IpRanges']:
                            if ip_range.get('CidrIp') == cidr_ip:
                                rule_exists= True
                                break
                if rule_exists:
                    break

            if rule_exists:
                print(f"Rule already exists: {ip_protocol}, {cidr_ip}, from port {from_port}, to port {to_port}")
                print("Continue")
            else:
                defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp=cidr_ip,
                IpProtocol=ip_protocol,
                FromPort=int(self.DWH_PORT),    
                ToPort=int(self.DWH_PORT)
            )   
            
        except Exception as error:
            print(f"Error to create access to endpoint:{error}")

    def deploy_cluster(self,redshift,iam,ec2):
        """Set the configuration and deploy a new cluster 

        Parameters
        ---------

        redshift : boto3.client
            A boto3 client instance for Amazon Redshift. Used to create and manage the Redshift cluster.
    
        iam : boto3.client
            A boto3 client instance for AWS Identity and Access Management (IAM). Used to configure roles and retrieve their ARNs.
    
        ec2 : boto3.resource
             A boto3 resource instance for Amazon EC2. Used to configure security groups and access rules.


        Returns
        --------
        bool
            Returns 'True in the ready variable if the cluster is successfully deployed and ready, otherwise returns False in the ready variable.
        
        Notes
        ------
            Ensure that the appropriate AWS credentials and permissions are configured for the `redshift`, `iam`, and `ec2` clients.
        """


        ready= False
        available = False
      
        try:
        
            self.set_roles_config(iam)

            roleArn =  self.get_ARN(iam)
            self.create_cluster(redshift,roleArn)

            available = self.check_available_cluster(redshift)

            if available:
                self.set_host_cluster()
                self.access_cluster_by_endpoint(ec2)

                print("Cluster is ready")
                ready= True
            else:
                print("Cluster is not available.Try again")

        except Exception as error:
            print(f"Error to depoly cluster {error}.Try again")

        return ready
        
    def delete_cluster(self,redshift):
        """Delete a cluster
        Parameters
        ---------

        redshift : boto3.client
            A boto3 client instance for Amazon Redshift. Used to create and manage the Redshift cluster.
        
        Returns
        -------
        None
            This function does not return a value.
    
        """

        redshift.delete_cluster( ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
