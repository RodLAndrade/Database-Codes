from sqlalchemy import create_engine
import json
import csv
import sys# FUNCOES PARA DELETAR TABELAS PRE EXISTENTES

def drop_table_pessoas_pgsql():
    return """DROP TABLE IF EXISTS pessoas"""

def drop_table_categorias_pgsql():
    return """DROP TABLE IF EXISTS categorias"""

def drop_table_produtos_pgsql():
    return """DROP TABLE IF EXISTS produtos"""

def drop_table_vendas_pgsql():
    return """DROP TABLE IF EXISTS vendas"""


# FUNCOES PARA DELETAR AS SEQUENCIAS DE ID PRE EXISTENTES

def drop_seq_table_pessoas_pgsql():
    return """DROP SEQUENCE IF EXISTS public.pessoas_id_seq;"""

def drop_seq_table_categorias_pgsql():
    return """DROP SEQUENCE IF EXISTS public.categorias_id_seq;"""

def drop_seq_table_produtos_pgsql():
    return """DROP SEQUENCE IF EXISTS public.produtos_id_seq;"""

def drop_seq_table_vendas_pgsql():
    return """DROP SEQUENCE IF EXISTS public.vendas_id_seq;"""


# FUNCOES PARA CRIAR AS SEQUENCIAS

def create_seq_table_pessoas_pgsql():
    return """CREATE SEQUENCE IF NOT EXISTS public.pessoas_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 10000
    CACHE 1;"""

def create_seq_table_categorias_pgsql():
    return """CREATE SEQUENCE IF NOT EXISTS public.categorias_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 10000
    CACHE 1;"""

def create_seq_table_produtos_pgsql():
    return """CREATE SEQUENCE IF NOT EXISTS public.produtos_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 10000
    CACHE 1;"""

def create_seq_table_vendas_pgsql():
    return """CREATE SEQUENCE IF NOT EXISTS public.vendas_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 10000
    CACHE 1;"""


# FUNCOES PARA CRIAR AS TABELAS

def create_pessoas_table_pgsql():
    return """CREATE TABLE pessoas (
    id int DEFAULT nextval('pessoas_id_seq')PRIMARY KEY,
    primeiro_nome VARCHAR(255) NOT NULL,
    ultimo_nome VARCHAR(255) NOT NULL,
    endereco VARCHAR(255) NOT NULL,
    cidade VARCHAR(255) NOT NULL
);"""

def create_categorias_table_pgsql(): 
    return """CREATE TABLE categorias (
    id int DEFAULT nextval('categorias_id_seq')PRIMARY KEY,
    nome VARCHAR(255) NOT NULL
) ;"""

def create_produtos_table_pgsql(): 
    return """CREATE TABLE produtos (
    id int DEFAULT nextval('produtos_id_seq')PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    preco DECIMAL(10,2) NOT NULL,
    id_categoria INT NOT NULL,
    FOREIGN KEY (id_categoria) REFERENCES categorias(id)
);"""

def create_vendas_table_pgsql(): 
    return """CREATE TABLE vendas (
    id int DEFAULT nextval('vendas_id_seq')PRIMARY KEY,
    id_pessoa INT NOT NULL,
    id_produto INT NOT NULL,
    quantidade INT NOT NULL,
    valor_total DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_pessoa) REFERENCES pessoas(id),
    FOREIGN KEY (id_produto) REFERENCES produtos(id)
);"""


# VARIAVEIS PARA INSERT DE DADOS NAS TABELAS

insert_pessoas_pgsql = """ INSERT INTO pessoas (primeiro_nome, ultimo_nome, endereco, cidade) VALUES
    LISTA_DE_DADOS  
""";

insert_categorias_pgsql = """ INSERT INTO categorias (nome) VALUES
    LISTA_DE_DADOS
""";

insert_produtos_pgsql = """INSERT INTO produtos (nome, preco, id_categoria) VALUES
    LISTA_DE_DADOS
""";

insert_vendas_pgsql = """INSERT INTO vendas (id_pessoa, id_produto, quantidade, valor_total) VALUES
    LISTA_DE_DADOS
""";


# EXECUTAR O CÓDIGO

pessoas = []
pessoas_nome = ''
pessoas_sobrenome = ''
pessoas_rua = ''
pessoas_cidade = ''

categorias = []
categorias_nome = ''

produtos = []
produtos_nome = ''
produtos_preco = ''
produtos_id_categoria = ''

vendas = []
vendas_id_pessoa = ''
vendas_id_produto = ''
vendas_quantidade = ''
vendas_valor_total = ''


if len(sys.argv) < 2:
    print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
    sys.exit(1)

tipo_arquivo = sys.argv[1].upper().strip()

if tipo_arquivo not in ["C", "J"]:
    print("Formato inválido. Use 'C' para CSV ou 'J' para JSON.")
    sys.exit(1)



try:
    engine = create_engine("postgresql://postgres:1234@localhost:5432/db_teste")
    if engine:
        print("Connected!")
        print()
except:
    print("Cannot connect!")

engine.connect()
with engine.connect() as con:
# EXECUTANDO OS DROP TABLES
    print("Deletando tabelas pré existentes...")
    print()
    con.execute(drop_table_vendas_pgsql())
    con.execute(drop_table_produtos_pgsql())
    con.execute(drop_table_categorias_pgsql())
    con.execute(drop_table_pessoas_pgsql())


    # EXECUTANDO OS DROP SEQUENCIAS

    con.execute(drop_seq_table_vendas_pgsql())
    con.execute(drop_seq_table_produtos_pgsql())
    con.execute(drop_seq_table_categorias_pgsql())
    con.execute(drop_seq_table_pessoas_pgsql())


    # EXECUTANDO OS CREATE SEQUENCIAS

    con.execute(create_seq_table_vendas_pgsql())
    con.execute(create_seq_table_produtos_pgsql())
    con.execute(create_seq_table_categorias_pgsql())
    con.execute(create_seq_table_pessoas_pgsql())


    # EXECUTANDO OS CREATE TABLES
    print("Criando novas tabelas..")
    print()
    con.execute(create_pessoas_table_pgsql())
    con.execute(create_categorias_table_pgsql())
    con.execute(create_produtos_table_pgsql())
    con.execute(create_vendas_table_pgsql())


    if tipo_arquivo == "C":

        print("Tipo de arquivo selecionado: CSV")

        with open("dados_tabela_pessoas.csv", mode = "r", encoding = "utf-8") as arq:
            leitor = csv.reader(arq, delimiter=',')

            for indice, linha in enumerate(leitor):
                if indice != 0:
                    pessoas_nome = linha[1]
                    pessoas_sobrenome = linha[2]
                    pessoas_rua = linha[3]
                    pessoas_cidade = linha[4]

                    pessoas = [{"primeiro_nome": pessoas_nome, "ultimo_nome": pessoas_sobrenome, "endereco": pessoas_rua, "cidade": pessoas_cidade}]

                    for pessoa in pessoas:
                        con.execute(
                            insert_pessoas_pgsql.replace(
                                "LISTA_DE_DADOS", 
                                f"('{pessoa['primeiro_nome']}', '{pessoa['ultimo_nome']}', '{pessoa['endereco']}', '{pessoa['cidade']}')"
                            )
                        )


        with open("dados_tabela_categorias.csv", mode = "r", encoding = "utf-8") as arq:
            leitor = csv.reader(arq, delimiter=',')

            for indice, linha in enumerate(leitor):
                if indice != 0:
                    categorias_nome = linha[1]

                    categorias = [{"nome": categorias_nome}]

                    for categoria in categorias:
                        con.execute(
                            insert_categorias_pgsql.replace(
                                "LISTA_DE_DADOS", 
                                f"('{categoria['nome']}')"
                            )
                        )


        with open("dados_tabela_produtos.csv", mode = "r", encoding = "utf-8") as arq:
            leitor = csv.reader(arq, delimiter=',')

            for indice, linha in enumerate(leitor):
                if indice != 0:
                    produtos_nome = linha[1]
                    produtos_preco = linha[2]
                    produtos_id_categoria = linha[3]

                    produtos = [{"nome": produtos_nome, "preco": produtos_preco, "id_categoria": produtos_id_categoria}]

                    for produto in produtos:
                        con.execute(
                            insert_produtos_pgsql.replace(
                                "LISTA_DE_DADOS", 
                                f"('{produto['nome']}', {produto['preco']}, {produto['id_categoria']})"
                            )
                        )


        with open("dados_tabela_vendas.csv", mode = "r", encoding = "utf-8") as arq:
            leitor = csv.reader(arq, delimiter=',')

            for indice, linha in enumerate(leitor):
                if indice != 0:
                    vendas_id_pessoa = linha[1]
                    vendas_id_produto = linha[2]
                    vendas_quantidade = linha[3]
                    vendas_valor_total = linha[4]

                    vendas = [{"id_pessoa": vendas_id_pessoa, "id_produto": vendas_id_produto, "quantidade": vendas_quantidade, "valor_total": vendas_valor_total}]

                    for venda in vendas:
                        con.execute(
                            insert_vendas_pgsql.replace(
                                "LISTA_DE_DADOS", 
                                f"({venda['id_pessoa']}, {venda['id_produto']}, {venda['quantidade']}, {venda['valor_total']})"
                            )
                        )
        print()                    
        print("Banco de dados criado com sucesso.")
        print("FIM")



    elif tipo_arquivo == "J":   

        print("Tipo de arquivo selecionado: JSON")

        if len(sys.argv) < 3:
            print("Parametros errados! Uso correto: python seu_programa.py <formato do arquivo (CSV = 'C' ou JSON = 'J') > [<opção (junto = 'J' ou separado = 'S'), apenas em caso de JSON>]")
            sys.exit(1)

        opcao_json = sys.argv[2].upper().strip()

        if opcao_json not in ["S", "J"]:
            print("Opção inválida para opção quantos arquivos JSON. Use 'S' para arquivos separados ou 'J' para um único arquivo.")
            sys.exit(1)
         


        if opcao_json == "S":

            print("Criando banco de dados com um arquivo por tabela...")

            with open("dados_tabela_pessoas.json", mode = "r", encoding = "utf-8") as arq:
                dados = json.load(arq)

                for dado in dados:
                    pessoas_nome = dado["primeiro_nome"]
                    pessoas_sobrenome = dado["ultimo_nome"]
                    pessoas_rua = dado["endereco"]
                    pessoas_cidade = dado["cidade"]

                    pessoas = [{"primeiro_nome": pessoas_nome, "ultimo_nome": pessoas_sobrenome, "endereco": pessoas_rua, "cidade": pessoas_cidade}]

                    for pessoa in pessoas:
                        con.execute(
                            insert_pessoas_pgsql.replace(
                                "LISTA_DE_DADOS", 
                                f"('{pessoa['primeiro_nome']}', '{pessoa['ultimo_nome']}', '{pessoa['endereco']}', '{pessoa['cidade']}')"
                            )
                        )

                            
            with open("dados_tabela_categorias.json", mode = "r", encoding = "utf-8") as arq:
                dados = json.load(arq)

                for dado in dados:
                    categorias_nome = dado["nome"]

                    categorias = [{"nome": categorias_nome}]

                    for categoria in categorias:
                        con.execute(
                            insert_categorias_pgsql.replace(
                                    "LISTA_DE_DADOS", 
                                f"('{categoria['nome']}')"
                            )
                        )


            with open("dados_tabela_produtos.json", mode = "r", encoding = "utf-8") as arq:
                dados = json.load(arq)

                for dado in dados:
                        produtos_nome = dado["nome"]
                        produtos_preco = dado["preco"]
                        produtos_id_categoria = dado["id_categoria"]

                        produtos = [{"nome": produtos_nome, "preco": produtos_preco, "id_categoria": produtos_id_categoria}]

                        for produto in produtos:
                            con.execute(
                                insert_produtos_pgsql.replace(
                                    "LISTA_DE_DADOS", 
                                    f"('{produto['nome']}', {produto['preco']}, {produto['id_categoria']})"
                                )
                            )


            with open("dados_tabela_vendas.json", mode = "r", encoding = "utf-8") as arq:
                dados = json.load(arq)

                for dado in dados:
                        vendas_id_pessoa = dado["id_pessoa"]
                        vendas_id_produto = dado["id_produto"]
                        vendas_quantidade = dado["quantidade"]
                        vendas_valor_total = dado["valor_total"]

                        vendas = [{"id_pessoa": vendas_id_pessoa, "id_produto": vendas_id_produto, "quantidade": vendas_quantidade, "valor_total": vendas_valor_total}]

                        for venda in vendas:
                            con.execute(
                                insert_vendas_pgsql.replace(
                                    "LISTA_DE_DADOS", 
                                    f"({venda['id_pessoa']}, {venda['id_produto']}, {venda['quantidade']}, {venda['valor_total']})"
                                )
                            )
            print()                    
            print("Banco de dados criado com sucesso.")
            print("FIM")

        if opcao_json == "J":

            print("Criando banco de dados com um arquivo único...")


            with open("dados.json", mode = "r", encoding = "utf-8") as arq:
                dados = json.load(arq)

                for key, values in dados.items():
                    if key == "categorias":
                    
    
                        for valor in values:
                            categorias_nome = valor["nome"]

                            categorias = [{"nome": categorias_nome}]

                            for categoria in categorias:
                                con.execute(
                                    insert_categorias_pgsql.replace(
                                            "LISTA_DE_DADOS", 
                                        f"('{categoria['nome']}')"
                                    )
                                )
                    
                    elif key == "pessoas":
                    
    
                        for valor in values:
                            pessoas_nome = valor["primeiro_nome"]
                            pessoas_sobrenome = valor["ultimo_nome"]
                            pessoas_rua = valor["endereco"]
                            pessoas_cidade = valor["cidade"]

                            pessoas = [{"primeiro_nome": pessoas_nome, "ultimo_nome": pessoas_sobrenome, "endereco": pessoas_rua, "cidade": pessoas_cidade}]

                            for pessoa in pessoas:
                                con.execute(
                                    insert_pessoas_pgsql.replace(
                                        "LISTA_DE_DADOS", 
                                        f"('{pessoa['primeiro_nome']}', '{pessoa['ultimo_nome']}', '{pessoa['endereco']}', '{pessoa['cidade']}')"
                                    )
                                )

                    elif key == "produtos":
                    
    
                        for valor in values:
                            produtos_nome = valor["nome"]
                            produtos_preco = valor["preco"]
                            produtos_id_categoria = valor["id_categoria"]

                            produtos = [{"nome": produtos_nome, "preco": produtos_preco, "id_categoria": produtos_id_categoria}]

                            for produto in produtos:
                                con.execute(
                                    insert_produtos_pgsql.replace(
                                        "LISTA_DE_DADOS", 
                                        f"('{produto['nome']}', {produto['preco']}, {produto['id_categoria']})"
                                    )
                                )

                    elif key == "vendas":
                    
    
                        for valor in values:
                            vendas_id_pessoa = valor["id_pessoa"]
                            vendas_id_produto = valor["id_produto"]
                            vendas_quantidade = valor["quantidade"]
                            vendas_valor_total = valor["valor_total"]

                            vendas = [{"id_pessoa": vendas_id_pessoa, "id_produto": vendas_id_produto, "quantidade": vendas_quantidade, "valor_total": vendas_valor_total}]

                            for venda in vendas:
                                con.execute(
                                    insert_vendas_pgsql.replace(
                                        "LISTA_DE_DADOS", 
                                        f"({venda['id_pessoa']}, {venda['id_produto']}, {venda['quantidade']}, {venda['valor_total']})"
                                    )
                                )
            print()                    
            print("Banco de dados criado com sucesso.")
            print("FIM")