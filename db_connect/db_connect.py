from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
import os
import json

def create_session():
    contact_point = "cassandra.us-west-2.amazonaws.com"

    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    cert_path = os.path.join(os.path.dirname(__file__), 'resources/sf-class2-root.crt')
    ssl_context.load_verify_locations(cert_path)
    ssl_context.verify_mode = CERT_REQUIRED

    with open('credentials.json', 'r') as file:
        credentials = json.load(file)

    auth_provider = PlainTextAuthProvider(username='shiqi-732-at-437956307664', password=credentials["password"])
    profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    
    cluster = Cluster(contact_points=[contact_point], port=9142, auth_provider=auth_provider,ssl_context=ssl_context,execution_profiles={EXEC_PROFILE_DEFAULT: profile})

    session = cluster.connect()

    return session
