from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
import xmltodict
import json
from database import engine

def save_xmls():
    # Directory containing XML files
    xml_dir = 'xml'
    nf = []

    for root, dirs, files in os.walk(xml_dir):
        for file in files:
            if file.endswith('.xml'):
                xml_file_path = os.path.join(root, file)
                                    
                with open(xml_file_path, 'r') as xml_file:
                    content_xml = xml_file.read() \
                        .replace('<?xml version="1.0" encoding="UTF-8"?>', '') \
                        .replace('<?xml version="1.0" encoding="utf-8"?>', '')
                    if not content_xml:
                        continue
                    content_json = xmltodict.parse(content_xml)
                
                #print(os.path.join(root, file))
                
                if 'procEventoNFe' in content_json:
                    nf.append(                    
                    {
                        'key_nf': content_json['procEventoNFe']['evento']['infEvento']['chNFe'],
                        'status': not 'cancelad' or not 'canc' in file.lower(),
                        'situacao': 'Evento de Cancelamento para o CPF: '+
                            content_json['procEventoNFe']['evento']['infEvento']['CPF']
                                if 'CPF' in content_json['procEventoNFe']['evento']['infEvento']
                                else content_json['procEventoNFe']['evento']['infEvento']['CNPJ']
                                    if 'CNPJ' in content_json['procEventoNFe']['evento']['infEvento']
                                    else 'nao informado',
                        'content_json': json.dumps(content_json),
                        'content_xml': content_xml,
                    })
                else:                       
                    nf_json = content_json['nfeProc']['NFe'] if 'nfeProc' in content_json else content_json['NFe']
                    
                    nf.append(                    
                    {
                        'date': nf_json['infNFe']['ide']['dhEmi'],
                        'key_nf': nf_json['infNFe']['@Id'][3:],
                        'status': not 'cancelad' in file.lower(),
                        'situacao': content_json['nfeProc']['protNFe']['infProt']['xMotivo']
                            if 'nfeProc' in content_json 
                            else 'Não Informado',
                        'content_json': json.dumps(content_json),
                        'content_xml': content_xml,
                        'ie_emissor': nf_json['infNFe']['emit']['IE']
                            if 'IE' in nf_json['infNFe']['emit']
                            else '',
                        'ie_destinatario': nf_json['infNFe']['dest']['IE']
                            if 'IE' in nf_json['infNFe']['dest']
                            else '',
                        'cnpj_cpf_emissor': nf_json['infNFe']['emit']['CPF']
                            if 'CPF' in nf_json['infNFe']['emit'] 
                            else nf_json['infNFe']['emit']['CNPJ'] 
                                if 'CNPJ' in nf_json['infNFe']['emit'] 
                                else '',                        
                        'cnpj_cpf_destinatario': nf_json['infNFe']['dest']['CNPJ'] 
                            if 'CNPJ' in nf_json['infNFe']['dest'] 
                            else nf_json['infNFe']['dest']['CPF'] 
                                if 'CPF' in nf_json['infNFe']['dest'] 
                                else '',
                        'razao_social_emissor': nf_json['infNFe']['emit']['xNome'],
                        'razao_social_destinatario': nf_json['infNFe']['dest']['xNome'],
                        'fantasia_emissor': nf_json['infNFe']['emit']['xFant']
                            if 'xFant' in nf_json['infNFe']['emit']
                            else '',
                        'email_destinatario': nf_json['infNFe']['dest']['email']
                            if 'email' in nf_json['infNFe']['dest']
                            else ''
                    })
                
    print('------------------------')
    print('Total de nfs:'+ str(len(nf)))
    df = pd.DataFrame(nf)
    
    # Define the chunk size
    chunksize = 1
    engx = engine
    
    # Insert data into the database in chunks using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            futures.append(executor.submit(insert_chunk, chunk, i, engx))

        for future in futures:
            future.result()
    
    print('------------------------')
    print("All chunks inserted.")
    

def insert_chunk(chunk, i, eng):
    try:
        chunk.to_sql('nf', eng, schema='cbx', if_exists='append', index=False)
    except Exception as ex:
        print(f'Position: {i} \nError inserting chunk: {ex.args}')
    
    
save_xmls()