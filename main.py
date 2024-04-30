import os
import pandas as pd
import xmltodict
import json
from database import engine

def save_xmls():
    # Directory containing XML files
    xml_dir = 'xml'
    nf = []

    # Iterate over XML files
    for filename in os.listdir(xml_dir):
        if filename.endswith('.xml'):
            xml_file_path = os.path.join(xml_dir, filename)
                                   
            with open(xml_file_path, 'r') as xml_file:
                content_xml = xml_file.read().replace('<?xml version="1.0" encoding="UTF-8"?>', '')
                content_json = xmltodict.parse(content_xml)
                
            #print(content_xml)
            #print(content_json)
                        
            cpf_cnpj_emissor = content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('emit', {}).get('CPF') if content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('emit', {}).get('CPF', {}) else content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('emit', {}).get('CNPJ')
            cpf_cnpj_dest = content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('dest', {}).get('CPF') if content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('dest', {}).get('CPF', {}) else content_json.get('nfeProc', {}).get('NFe', {}).get('infNFe', {}).get('dest', {}).get('CNPJ')            
            
            nf.append(
                {
                'date': content_json['nfeProc']['NFe']['infNFe']['ide']['dhEmi'],
                'key_nf': content_json['nfeProc']['protNFe']['infProt']['chNFe'],
                'status': True,
                'situacao': content_json['nfeProc']['protNFe']['infProt']['xMotivo'],
                'content_json': json.dumps(content_json),
                'content_xml': content_xml,
                'ie_emissor': content_json['nfeProc']['NFe']['infNFe']['emit']['IE'],
                'ie_destinatario': content_json['nfeProc']['NFe']['infNFe']['dest']['IE'],
                'cnpj_cpf_emissor': cpf_cnpj_emissor,
                'cnpj_cpf_destinatario': cpf_cnpj_dest,
                'razao_social_emissor': content_json['nfeProc']['NFe']['infNFe']['emit']['xNome'],
                'razao_social_destinatario': content_json['nfeProc']['NFe']['infNFe']['dest']['xNome'],
                'fantasia_emissor': content_json['nfeProc']['NFe']['infNFe']['emit']['xFant'],
                'email_destinatario': content_json['nfeProc']['NFe']['infNFe']['dest']['email']
             })
    
    df = pd.DataFrame(nf)
    
    # Define the chunk size
    chunksize = 50

    # Insert data into the database in chunks
    for i in range(0, len(df), chunksize):
        try:
            chunk = df.iloc[i:i+chunksize]
            chunk.to_sql('nf', engine, schema='cbx', if_exists='append', index=False)
        except Exception as ex:
            print('******************************\nposition: '+str(i)+'\n'+str(ex))

save_xmls()