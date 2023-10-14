from sqlalchemy import create_engine
from random import choice, randint
import pymongo as pm
import pandas as pd
import sys
import json


# Listas e dicionarios com dados para alimentar as tabelas

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




try:
    connection_string = 'mongodb+srv://rodandrade:1234@cluster0.olgokvo.mongodb.net/'
    client = pm.MongoClient(connection_string)
    if client:
        print("Connected!")
        print()
except:
    print("Cannot connect!")

db = client.get_database('estudo')

# DELETANDO COLEÇÕES PRÉ EXISTENTES

if 'pessoas' in db.list_collection_names():
    db['pessoas'].drop()
if 'categorias' in db.list_collection_names():
    db['categorias'].drop()
if 'produtos' in db.list_collection_names():
    db['produtos'].drop()
if 'vendas' in db.list_collection_names():
    db['vendas'].drop()

#CRIANDO NOVAS COLEÇÕES

collection_pessoas = db.create_collection('pessoas')
collection_categorias = db.create_collection('categorias')
collection_produtos = db.create_collection('produtos')
collection_vendas = db.create_collection('vendas')

#LISTAS E DICIONARIOS PARA ARMAZENAR OS DADOS GERADOS

tabelas_criadas = {}
tabela_pessoas = []
tabela_categorias = []
tabela_produtos = []
tabela_vendas = []


print("Gerando os dados...")
print()
for c in range(0, 10000):  
        
    # variaveis para e insert de dados na tabela pessoas e categorias

    cidade_escolhida = choice(cidades)
    lista_de_ruas = ruas.get(cidade_escolhida)
    pessoas = {"id": c+1, "primeiro_nome": choice(primeiros_nomes), "ultimo_nome": choice(sobrenomes), "endereco": choice(lista_de_ruas), "cidade": cidade_escolhida}
    tabela_pessoas.append(pessoas)


    categorias = {"id": c+1, "nome": choice(tipos_de_categorias)}
    tabela_categorias.append(categorias)

tabelas_criadas['pessoas'] = tabela_pessoas
tabelas_criadas['categorias'] = tabela_categorias

collection_pessoas.insert_many(tabela_pessoas)
collection_categorias.insert_many(tabela_categorias)

for c in range(0, 10000):

    # variaveis para e insert de dados na tabela produtos

    qual_id_categoria = randint(0, 9999)
    categoria_randomizada = tabelas_criadas['categorias'][qual_id_categoria]
    nome_categoria_randomizada = categoria_randomizada['nome']
    produto = tipos_de_produtos[nome_categoria_randomizada]

    produtos = {"id": c+1, "nome": choice(produto),"preco": randint(1, 999),"id_categoria": qual_id_categoria+1}
    tabela_produtos.append(produtos)

tabelas_criadas['produtos'] = tabela_produtos
collection_produtos.insert_many(tabela_produtos)


for c in range(0, 10000):

    # variaveis para e insert de dados na tabela vendas

    qual_idproduto_por_em_vendas = randint(0, 9999)
    idproduto_selecionado = tabelas_criadas['produtos'][qual_idproduto_por_em_vendas]
    preco_idproduto_selecionado = idproduto_selecionado['preco']
    qual_quantidade = randint(1, 100)
    qual_valor_total = qual_quantidade * preco_idproduto_selecionado
    
    vendas = {"id": c+1, "id_pessoa": randint(1, 10000), "id_produto": qual_idproduto_por_em_vendas+1, "quantidade": qual_quantidade, "valor_total": qual_valor_total}
    tabela_vendas.append(vendas)
    
tabelas_criadas['vendas'] = tabela_vendas
collection_vendas.insert_many(tabela_vendas)

print()                    
print("Banco de dados criado com sucesso.")
print("FIM")