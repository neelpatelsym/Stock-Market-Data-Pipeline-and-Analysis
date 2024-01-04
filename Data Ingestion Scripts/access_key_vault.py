import os
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

load_dotenv()

client_id = os.environ.get('AZURE_CLIENT_ID')
tenant_id = os.environ.get('AZURE_TENANT_ID')
client_secret = os.environ.get('AZURE_CLIENT_SECRET')
vault_url = os.environ.get('AZURE_VAULT_URL')

secret_name = ['connection-str','eventhub-name']

# creating a credentials object
credentials = ClientSecretCredential(
    client_id = client_id,
    client_secret = client_secret,
    tenant_id = tenant_id
)

secret_client =SecretClient(vault_url = vault_url, credential = credentials)

connection_str = secret_client.get_secret(secret_name[0])

eventhub_name = secret_client.get_secret(secret_name[1])
