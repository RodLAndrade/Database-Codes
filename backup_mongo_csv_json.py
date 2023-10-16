import bson.json_util as json_util
import pymongo as pm
import pandas as pd
import sys
import json

# VALIDANDO OS PARAMETROS INICIAIS

if len(sys.argv) < 2:
    print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
    sys.exit(1)
tipo_arquivo = sys.argv[1].upper().strip()
if tipo_arquivo not in ["C", "J"]:
    print("Formato inválido. Use 'C' para CSV ou 'J' para JSON.")
    sys.exit(1)

#   EXECUTAR O CÓDIGO

try:
    connection_string = 'mongodb+srv://rodandrade:1234@cluster0.olgokvo.mongodb.net/'
    client = pm.MongoClient(connection_string)
    if client:
        print("Connected!")
        print()
except:
    print("Cannot connect!")

db = client.get_database('estudo')
collections_a_salvar = db.list_collection_names()

if tipo_arquivo == "C": #CSV
    print("Tipo de arquivo selecionado: CSV")
    for collection in collections_a_salvar:
        col = db.get_collection(collection)

        cursor = col.find()
        df = pd.DataFrame(cursor)
        df.to_csv(f"dados_collection_{collection}.csv", index=False)



elif tipo_arquivo == "J": #JSON

    print("Tipo de arquivo selecionado: JSON")
    print()

    if len(sys.argv) < 3:
        print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
        sys.exit(1)
    opcao_json = sys.argv[2].upper().strip()

    if opcao_json not in ["S", "J"]:
        print("Opção inválida para opção quantos arquivos JSON. Use 'S' para arquivos separados ou 'J' para um único arquivo.")
        sys.exit(1)

    if opcao_json == "S": #JSON EM ARQUIVOS SEPARADOS

        print("Criando um arquivo por tabela...")

        for collection in collections_a_salvar:
            col = db.get_collection(collection)

            cursor = col.find()
            docs = list(cursor)
            serialized_docs = json.loads(json_util.dumps(docs))

            with open(f"dados_collection_{collection}.json", 'w', encoding='utf8') as json_file:
                json_file.write(json.dumps(serialized_docs, ensure_ascii=False))



    elif opcao_json == "J": #JSON EM ARQUIVOS JUNTOS

        print("Criando arquivo único...") 

        dicionarios = {}
        for collection in collections_a_salvar:
            col = db.get_collection(collection)

            cursor = col.find()
            docs = list(cursor)
            serialized_docs = json.loads(json_util.dumps(docs))
            dicionarios[collection] = serialized_docs

        with open(f"dados_collections.json", 'w', encoding='utf8') as json_file:
            json_file.write(json.dumps(dicionarios, ensure_ascii=False))


print()                    
print("Arquivos criados com sucesso.")
