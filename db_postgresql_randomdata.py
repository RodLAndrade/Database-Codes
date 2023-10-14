from sqlalchemy import create_engine
import psycopg2 as pg
from random import choice, randint

# LISTAS E DICIONARIOS COM DADOS PARA ALIMENTAR AS TABELAS

primeiros_nomes = [
    "João", "Maria", "Pedro", "Ana", "Lucas", "Carla", "Mateus", "Juliana", "Gabriel", "Larissa",
    "Felipe", "Camila", "Luiz", "Mariana", "Guilherme", "Patrícia", "Gustavo", "Amanda", "Enzo", "Beatriz",
    "Rafael", "Laura", "Matheus", "Isabela", "Carlos", "Renata", "Daniel", "Lívia", "Bruno", "Fernanda",
    "Leonardo", "Natália", "Eduardo", "Larissa", "Vitor", "Lúcia", "Marcos", "Sofia", "Thiago", "Valentina",
    "Samuel", "Letícia", "Diego", "Clara", "Antônio", "Gabriela", "Fernando", "Elisa", "Ricardo", "Marina",
    "Alexandre", "Luana", "André", "Fernanda", "José", "Raquel", "Francisco", "Jéssica", "Vinícius", "Bianca",
    "Raul", "Tatiana", "Cauã", "Vanessa", "Vinícius", "Cristina", "Miguel", "Talita", "Caio", "Valéria",
    "Felipe", "Roberta", "Renato", "Júlia", "Rodrigo", "Lorena", "Márcio", "Caroline", "Wagner", "Elaine",
    "Leandro", "Cláudia", "Hugo", "Fátima", "Arthur", "Mônica", "Ronaldo", "Rosa", "Davi", "Viviane",
    "Luciano", "Adriana", "Valmir", "Aline"
]

sobrenomes = [
    "Silva", "Santos", "Oliveira", "Pereira", "Almeida", "Ferreira", "Cavalcanti", "Ribeiro", "Carvalho", "Gomes",
    "Rodrigues", "Melo", "Costa", "Martins", "Araújo", "Barbosa", "Fernandes", "Nascimento", "Lima", "Azevedo",
    "Vieira", "Correia", "Rocha", "Cardoso", "Mendes", "Dias", "Souza", "Barros", "Monteiro", "Castro",
    "Moura", "Diniz", "Freitas", "Campos", "Ramos", "Guimarães", "Tavares", "Sousa", "Cunha", "Machado",
    "Farias", "Teixeira", "Marques", "Lopes", "Fonseca", "Andrade", "Correia", "Gonçalves", "Dantas", "Cruz",
    "Siqueira", "Figueiredo", "Moraes", "Peixoto", "Cavalcante", "Reis", "Leite", "Pinto", "Pimentel", "Aragão",
    "Lopes", "Brito", "Bezerra", "Freire", "Calado", "Vasconcelos", "Xavier", "Abreu", "Medeiros", "Queiroz",
    "Peixoto", "Macedo", "Pessoa", "Valente", "Valença", "Coutinho", "Sá", "Magalhães", "Pontes", "Lins",
    "Santiago", "Cordeiro", "Figueira", "Gusmão", "Saldanha", "Porto", "Nogueira", "Paiva", "Albuquerque", "Assis",
    "Alcântara", "Sobral", "Duarte", "Viana", "Padilha", "Lameira", "Rebelo", "Tavares", "Mota", "Vidal"
]

cidades = [
    "Rio Branco", "Maceió", "Manaus", "Salvador", "Fortaleza", "Vitória", "Goiânia", "São Luís", "Belo Horizonte", "Belém", "João Pessoa", "Curitiba",
    "Rio de Janeiro", "Florianópolis", "São Paulo", "Ourinhos"
]

ruas = {
    "Rio Branco" : ["Avenida Ceará", "Rua Isaura Parente", "Rua Quintino Bocaiúva", "Avenida Brasil", "Rua Benjamin Constant"],         
    "Maceió" : ["Avenida Fernandes Lima", "Rua Sá e Albuquerque", "Avenida Comendador Gustavo Paiva", "Rua Jangadeiros Alagoanos", "Avenida Álvaro Otacílio"], 
    "Manaus"  :["Avenida Eduardo Ribeiro", "Rua 24 de Maio", "Avenida Sete de Setembro", "Avenida Constantino Nery", "Avenida Djalma Batista"], 
    "Salvador" : ["Avenida Sete de Setembro", "Rua Castro Alves", "Avenida Tancredo Neves", "Avenida ACM", "Avenida Garibaldi"],
    "Fortaleza" : ["Avenida Beira Mar", "Rua Monsenhor Tabosa", "Avenida Dom Luís", "Rua Barão de Studart", "Avenida Heráclito Graça"],
    "Vitória" : ["Avenida Jerônimo Monteiro", "Rua Sete de Setembro", "Avenida Princesa Isabel", "Rua Duque de Caxias", "Avenida Leitão da Silva"],
    "Goiânia" : ["Avenida 85", "Rua 7 de Setembro", "Avenida T-63", "Rua 24 de Outubro", "Avenida Anhanguera"],
    "São Luís" : ["Avenida Litorânea", "Rua Grande", "Avenida Daniel de La Touche", "Rua Portugal", "Avenida Colares Moreira"],
    "Belo Horizonte" : ["Avenida Afonso Pena", "Rua da Bahia", "Avenida Amazonas", "Avenida Contorno", "Avenida do Contorno"],
    "Belém" : ["Avenida Presidente Vargas", "Avenida Nazaré", "Rua dos Mundurucus", "Avenida Almirante Barroso", "Travessa Quintino Bocaiúva"],
    "João Pessoa" : ["Avenida Epitácio Pessoa", "Rua Capitão José Pessoa", "Avenida Senador Ruy Carneiro", "Rua Diogo Velho", "Avenida Presidente Getúlio Vargas"],
    "Curitiba" : ["Avenida Batel", "Rua XV de Novembro", "Avenida República Argentina", "Rua Marechal Deodoro", "Avenida Sete de Setembro"],
    "Rio de Janeiro" : ["Avenida Atlântica", "Rua Visconde de Pirajá", "Avenida Rio Branco", "Rua Copacabana", "Avenida Presidente Vargas"],
    "Florianópolis" : ["Avenida Beira-Mar Norte", "Rua João Pinto", "Avenida Hercílio Luz", "Rua Esteves Júnior", "Avenida Mauro Ramos"],
    "São Paulo" : ["Avenida Paulista", "Rua Augusta", "Avenida Brigadeiro Faria Lima", "Avenida Rebouças", "Rua Oscar Freire"],
    "Ourinhos" : ["Rua Cardoso Ribeiro", "Rua Cambará", "Rua Paraná", "Rua dos Expedicionários", "Rua Argemiro Geraldo"]
}

tipos_de_categorias = [
    "Eletrônicos", "Roupas e Moda", "Alimentos e Bebidas", "Móveis e Decoração", "Produtos de Limpeza e Higiene", "Brinquedos e Jogos", "Eletrodomésticos", 
    "Saúde e Beleza", "Material de Escritório e Escolar", "Ferramentas e Equipamentos"
]

tipos_de_produtos = {
    "Eletrônicos" : [
        "Smartphone Samsung Galaxy S21", "Notebook Dell Inspiron 15","TV LED LG 55 polegadas", "Console de Video Game Xbox Series X", "Fones de Ouvido Sony WH-1000XM4", 
        "Câmera DSLR Canon EOS 90D", "Tablet Apple iPad Pro", "Smartwatch Garmin Fenix 6","Impressora HP LaserJet Pro","Roteador Wi-Fi TP-Link Archer C4000"
], 
    "Roupas e Moda": [
        "Vestido de Verão Floral", "Tênis Nike Air Max", "Camisa Polo Ralph Lauren", "Calça Jeans Levis 501", "Bolsa de Couro Genuíno", "Blazer Slim Fit", 
        "Sapato Social Clássico", "Óculos de Sol Ray-Ban Aviator", "Pijama de Algodão", "Moletom com Capuz Adidas"
],
    "Alimentos e Bebidas": [
        "Arroz Integral", "Feijão Preto", "Macarrão Espaguete", "Leite Desnatado", "Salada Pronta de Alface e Tomate", "Cerveja Artesanal IPA", "Suco de Laranja Natural", "Chocolate Amargo 70", "Azeite de Oliva Extra Virgem", "Iogurte Grego"
],
    "Móveis e Decoração": [
        "Sofá de Couro", "Mesa de Jantar de Madeira", "Cama King Size Estofada", "Luminária de Chão", "Estante para Livros", "Mesa de Centro de Vidro", "Tapete de Lã",
        "Quadro Abstrato", "Cadeira de Escritório Ergonômica", "Espelho Decorativo"
],
    "Produtos de Limpeza e Higiene": [
        "Detergente Líquido", "Sabonete Líquido", "Papel Higiênico", "Amaciante de Roupas", "Desinfetante Multiuso", "Escova de Dentes Elétrica", "Shampoo Suave",
        "Esponja de Banho", "Limpador de Vidros", "Creme Dental Branqueador"
],
    "Brinquedos e Jogos": [
        "Lego Classic 1500 Peças", "Boneca Barbie Fashionista", "Quebra-Cabeça 1000 Peças", "Jogo de Tabuleiro Monopoly", "Carro de Controle Remoto",
        "Bicicleta Infantil", "Kit de Pintura a Dedo", "Pelúcia de Ursinho de Pelúcia", "Kit de Ciência para Crianças", "Patinete Dobrável"
],
    "Eletrodomésticos": [
        "Geladeira Side by Side", "Fogão de Indução", "Máquina de Lavar Roupa Frontal", "Micro-ondas Inox", "Aspirador de Pó Robô", "Liquidificador Potente",
        "Cafeteira Elétrica", "Forno Elétrico de Embutir", "Máquina de Café Espresso", "Secadora de Roupas"
],
    "Saúde e Beleza": [
        "Perfume Chanel Nº 5", "Creme Anti-idade La Prairie", "Escova de Cabelo Tangle Teezer", "Protetor Solar Nivea FPS 50", "Máscara Facial de Argila",
        "Kit de Maquiagem Profissional", "Aparelho de Barbear Gillette Fusion", "Esmalte de Unhas Essie", "Creme Hidratante Corporal Neutrogena", "Suplemento de Colágeno"
],
    "Material de Escritório e Escolar": [
        "Notebook Lenovo ThinkPad", "Canetas Coloridas Stabilo", "Caderno Universitário Espiral", "Mochila Executiva", "Calculadora Científica HP", "Agenda 2023",
        "Pasta de Arquivos Expansível", "Cadeira Ergonômica de Escritório", "Projetor Epson PowerLite", "Apontador Elétrico"
],
    "Ferramentas e Equipamentos": [
        "Furadeira de Impacto Bosch", "Chave de Fenda e Philips Set", "Serra Circular Makita", "Escada de Alumínio Telescópica", "Compressor de Ar Portátil",
        "Jogo de Chaves de Soquete", "Lixadeira Orbital", "Multímetro Digital", "Alicate de Corte", "Conjunto de Brocas para Metal"
]}



# FUNCOES PARA DELETAR TABELAS PRE EXISTENTES

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

try:
    conexao = pg.connect(database = "db_teste",host = "localhost",user = "postgres",password = "1234",port = "5432")
    curs = conexao.cursor()
    if curs:
        print("Connected!")
        print()
except:
    print("Cannot connect!")


# EXECUTANDO OS DROP TABLES
print("Deletando tabelas pré existentes...")
print()
curs.execute(drop_table_vendas_pgsql())
curs.execute(drop_table_produtos_pgsql())
curs.execute(drop_table_categorias_pgsql())
curs.execute(drop_table_pessoas_pgsql())


# EXECUTANDO OS DROP SEQUENCIAS

curs.execute(drop_seq_table_vendas_pgsql())
curs.execute(drop_seq_table_produtos_pgsql())
curs.execute(drop_seq_table_categorias_pgsql())
curs.execute(drop_seq_table_pessoas_pgsql())


# EXECUTANDO OS CREATE SEQUENCIAS

curs.execute(create_seq_table_vendas_pgsql())
curs.execute(create_seq_table_produtos_pgsql())
curs.execute(create_seq_table_categorias_pgsql())
curs.execute(create_seq_table_pessoas_pgsql())


# EXECUTANDO OS CREATE TABLES
print("Criando novas tabelas..")
print()
curs.execute(create_pessoas_table_pgsql())
curs.execute(create_categorias_table_pgsql())
curs.execute(create_produtos_table_pgsql())
curs.execute(create_vendas_table_pgsql())

tabelas_criadas = {}
tabela_pessoas = []
tabela_categorias = []
tabela_produtos = []
tabela_vendas = []

print("Gerando os dados...")
print()
for c in range(0, 10000):  
        
    # variaveis para e insert de dados na tabela pessoas

    cidade_escolhida = choice(cidades)
    lista_de_ruas = ruas.get(cidade_escolhida)
    pessoas = {"primeiro_nome": choice(primeiros_nomes), "ultimo_nome": choice(sobrenomes), "endereco": choice(lista_de_ruas), "cidade": cidade_escolhida}
    tabela_pessoas.append(pessoas)

    curs.execute(
        insert_pessoas_pgsql.replace(
            "LISTA_DE_DADOS", 
            f"('{pessoas['primeiro_nome']}', '{pessoas['ultimo_nome']}', '{pessoas['endereco']}', '{pessoas['cidade']}')"
        )
    )

    categorias = {"nome": choice(tipos_de_categorias)}
    tabela_categorias.append(categorias)

    curs.execute(
        insert_categorias_pgsql.replace(
            "LISTA_DE_DADOS", 
            f"('{categorias['nome']}')"
        )
    )
    
tabelas_criadas['pessoas'] = tabela_pessoas
tabelas_criadas['categorias'] = tabela_categorias


for c in range(0, 10000):  
        
    # variaveis para o insert de dados na tabela produtos

    qual_id_categoria = randint(0, 9999)
    categoria_randomizada = tabelas_criadas['categorias'][qual_id_categoria]
    nome_categoria_randomizada = categoria_randomizada['nome']
    produto = tipos_de_produtos[nome_categoria_randomizada]

    produtos = {"nome": choice(produto),"preco": randint(1, 999),"id_categoria": qual_id_categoria+1}
    tabela_produtos.append(produtos)

    curs.execute(
        insert_produtos_pgsql.replace(
            "LISTA_DE_DADOS", 
            f"('{produtos['nome']}', {produtos['preco']}, {produtos['id_categoria']})"
        )
    )

tabelas_criadas['produtos'] = tabela_produtos


for c in range(0, 10000):

    # variaveis para o insert de dados na tabela vendas

    qual_idproduto_por_em_vendas = randint(0, 9999)
    idproduto_selecionado = tabelas_criadas['produtos'][qual_idproduto_por_em_vendas]  
    preco_idproduto_selecionado = idproduto_selecionado['preco']
    qual_quantidade = randint(1, 100)
    qual_valor_total = qual_quantidade * preco_idproduto_selecionado

    vendas = {"id_pessoa": randint(1, 10000), "id_produto": qual_idproduto_por_em_vendas+1, "quantidade": qual_quantidade, "valor_total": qual_valor_total}

    curs.execute(
        insert_vendas_pgsql.replace(
            "LISTA_DE_DADOS", 
            f"({vendas['id_pessoa']}, {vendas['id_produto']}, {vendas['quantidade']}, {vendas['valor_total']})"
        )
    )

conexao.commit()  
print("Banco de dados criado com sucesso.")
print()   
print("FIM")