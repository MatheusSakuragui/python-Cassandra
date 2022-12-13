import pymongo
from pymongo.server_api import ServerApi

import random
import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


cloud_config= {
         'secure_connect_bundle': 'secure-connect-mercadolivre.zip'
}
auth_provider = PlainTextAuthProvider('PmgtSWmYtwzbXQiQaHaFifxa', 'IUZhgifnKj_Fc46,Nh.fdZFKHDFNFw16,iZxZDrqB6W,rJZb3oLamm_ijnqFKwJdJBb8NUeSXs892ZLwyAM.ZkLvJTpAM9h-7ZD_8Zz.ckKZJOZPFJZK12_4e2y9dHTj')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

session = cluster.connect()


session.execute("USE mercadolivre;")
print("Criando tabelas")

tabelas = session.execute("SELECT * FROM system_schema.tables where keyspace_name = 'mercadolivre'")


session.execute(
    "CREATE TABLE IF NOT EXISTS usuario (email text PRIMARY KEY, nome text, senha text, cpf text, endereco text, favoritos text);")
print("Criado a tabela de usuário")

session.execute(
    "CREATE TABLE IF NOT EXISTS vendedor (email text PRIMARY KEY, nome text, cnpj text);")
print("Criado a tabela de vendedor")

session.execute(
    "CREATE TABLE IF NOT EXISTS produto (id text PRIMARY KEY, nome text, preco text, descricao text, vendedor text);")
print("Criado a tabela de produto")

session.execute(
    "CREATE TABLE IF NOT EXISTS compra (id text PRIMARY KEY, usuario text, vendedor text, produtos text);")
print("Criado a tabela de compra")

client = pymongo.MongoClient("mongodb+srv://Sakuragui:mmmpt2022@cluster0.xksehma.mongodb.net/mercadolivre")
db = client.test

global mydb
mydb = client.mercadolivre

def insertVendedor(nome,email,cnpj):
    query = session.prepare("INSERT INTO vendedor( email, nome, cnpj) values (?, ?, ?)")
    session.execute(query,[email,nome,cnpj])
    print("\n####INSERT VENDEDOR####")

def updateVendedor(email,nome,cnpj):
    #Update Vendedor
    query = session.prepare("UPDATE vendedor SET nome = '{valor1}', cnpj = '{valor2}'  WHERE email = '{valor3}' ".format(valor1=nome, valor2=cnpj, valor3=email))
    session.execute(query)
    print("\n####UPDATE VENDEDOR####")

def deleteVendedor(vendedor):
    #Delete Vendedor
    global mydb
    mycol = mydb.vendedor
    mycol.delete_one(vendedor)

def insertUsuario(nome,cpf,email,senha,endereco):
    endereco = json.dumps(endereco)
    query = session.prepare("INSERT INTO usuario(email, nome, cpf, senha, endereco) values (?, ?, ?, ?, ?)")
    session.execute(query,[email,nome,cpf,senha,endereco])
    print("\n####INSERT USUARIO####")

def updateUsuario(emailUsu,nome,cpf,senha,endereco):
    endereco = json.dumps(endereco)
    query = session.prepare("UPDATE usuario SET nome = '{valor1}', cpf = '{valor2}' , senha = '{valor4}', endereco = '{valor5}' WHERE email = '{valor6}' ".format(valor1=nome, valor2=cpf, valor4=senha, valor5=endereco, valor6 = emailUsu))
    session.execute(query)
   
    print("\n####UPDATE USUARIO####")

def deleteUsuario(usuario):
    #Delete Usuario
    global mydb
    mycol = mydb.usuario
    mycol.delete_one(usuario)

def insertProduto(vendedor,nome,preco,descricao):
    id = str(random.randint(1, 100000))
    query = session.prepare("INSERT INTO produto(id, nome, preco, descricao, vendedor) values (?, ?, ?, ?, ?)")
    session.execute(query,[id,nome,preco,descricao,vendedor])
    print("\n####INSERT PRODUTO####")


def updateProduto(id,nome,preco,descricao):
    #Update Produto
    query = session.prepare("UPDATE produto SET nome = '{valor1}', preco = '{valor2}', descricao = '{valor3}' WHERE id = '{valor4}' ".format(valor1=nome, valor2=preco, valor3=descricao, valor4=id))
    session.execute(query)
    print("\n####UPDATE PRODUTO####")

def deleteProduto(produto):
    #Delete Produto
    global mydb
    mycol = mydb.produto
    mycol.delete_one(produto)

def findAllVendedores():
    vendedores = session.execute("SELECT * FROM vendedor")
    lista = []
    index = 0
    for vendedor in vendedores:
        print("Index do usuário {index}\nNome: {nome}\nEmail: {email}\nCnpj: {cnpj}\n".format(
            index=index, nome=vendedor.nome, email=vendedor.email, cnpj=vendedor.cnpj
        ))
        lista.append(vendedor.email)
        index += 1
    return lista

def findVendedor(id,email):
    #Query Vendedor
     
    if id:
        query = session.prepare(
        "SELECT * FROM vendedor WHERE id = ?")
        vendedor = session.execute(query, [id])
  
    else: 
        query = session.prepare(
        "SELECT * FROM vendedor WHERE email = ?")
        vendedor = session.execute(query, [email])
    for info in vendedor:
        print(
            f"""\nInformações \nNome: {info.nome}\nEmail: {info.email}\nCNPJ: {info.cnpj}""")
    return print("QUERY VENDEDOR")

def findAllUsuarios():
    usuarios = session.execute("SELECT * FROM usuario")
    lista = []
    index = 0
    for usuario in usuarios:
        print("Index do usuario {index}\nNome: {nome}\nEmail: {email}\nCpf: {cpf}\n".format(
            index=index, nome=usuario.nome, email=usuario.email, cpf=usuario.cpf
        ))
        lista.append(usuario.email)
        index += 1
    return lista

def findUsuario(id,email):
    #Query Usuario
   
    if id:
        query = session.prepare(
        "SELECT * FROM usuario WHERE id = ?")
        usuario = session.execute(query, [id])
  
    else: 
        query = session.prepare(
        "SELECT * FROM usuario WHERE email = ?")
        usuario = session.execute(query, [email])


    for info in usuario:
        print(
            f"""\nInformações \nNome: {info.nome}\nEmail: {info.email}\nCPF: {info.cpf}""")
        end = info.endereco
        end = json.loads(end)
        print( f"""\nInformações do Endereço \nCEP: {end["cep"]}\nRua: {end["rua"]}\nCidade: {end["cidade"]} \nEstado: {end["estado"]}""")
    return print("QUERY USUARIO")

def findAllProdutos():
    produtos = session.execute("SELECT * FROM produto")
    lista = []
    index = 0
    for produto in produtos:
        print("Index do produto {index}\nNome: {nome}\nPreco: {preco}\n".format(
            index=index, nome=produto.nome, preco=produto.preco
        ))

        lista.append({'id': produto.id, 'nome': produto.nome, 'vendedor': produto.vendedor})
        index += 1
    return lista

def findProduto(vendedor):
    query = session.prepare(
        "SELECT * FROM produto WHERE vendedor = ? limit 1000 ALLOW FILTERING")
    produto = session.execute(query, [vendedor])


    for info in produto:
        print(
            f"""\nInformações \nNome: {info.nome}\nPreço: {info.preco}\nEmail do Vendedor: {info.vendedor}""")
      
    return print("QUERY PRODUTO")

def findCompra(emailUsuario,emailVendedor):

    if emailUsuario:
        query = session.prepare(
            "SELECT * FROM compra WHERE usuario = ? limit 1000 ALLOW FILTERING")
        produto = session.execute(query, [emailUsuario])
    else:
        query = session.prepare(
            "SELECT * FROM compra WHERE vendedor = ? limit 1000 ALLOW FILTERING")
        produto = session.execute(query, [emailVendedor])
    for info in produto:
        print(
            f"""\nInformações da Compra \nVendedor: {info.vendedor}\n Comprador: {info.usuario}\nProduto: {info.produtos}""")
      
    return print("QUERY COMPRA")

def insertCompra(usuario,vendedor,produto):
    
    id = str(random.randint(1, 100000))
    produto = json.dumps(produto)
    query = session.prepare("INSERT INTO compra(id, usuario, vendedor, produtos) values (?, ?, ?, ?)")
    session.execute(query,[id,usuario,vendedor,produto])
    print("\n####INSERT COMPRA####")


############# main

def menu():
  
    
    loop = True
    while loop:
        print("""
            1 - Novo Usuario \n
            2 - Novo Vendedor \n 
            3 - Novo Produto \n
            4 - Realizar Compra \n
            5 - Encontrar Usuario \n
            6 - Encontrar Vendedor \n
            7 - Encontrar Produto \n
            8 - Encontrar Compra \n
            9 - Atualizar Usuario \n
            10 - Atualizar Vendedor \n
            11 - Atualizar Produto \n
            12 - Deletar Usuario \n
            13 - Deletar Vendedor \n
            14 - Deletar Produto \n
            0 - Sair \n
        """)
        escolha = input("Digite a Operação desejada: ")
        match escolha:
            case '1':
                nome = input("Insira o NOME do usuário : ")
                cpf = input("Insira o CPF do usuário: ")
                email = input("Insira o EMAIL do usuário: ")
                senha = input("Insira a SENHA do usuário: ")
                print("-----ENDEREÇO-----")
                cep = input("Insira o CEP do usuário: ")
                estado = input("Insira o ESTADO do usuário: ")
                cidade = input("Insira a CIDADE do usuário: ")
                rua = input("Insira a RUA do usuário: ")
                numero = input("Insira o NÚMERO do usuário: ")
                insertUsuario(nome,cpf,email,senha,{"cep":cep, "estado": estado, "cidade": cidade, "rua": rua, "numero": numero})
            case '2':
                nome = input("Insira o NOME do vendedor: ")
                email = input("Insira o EMAIL do vendedor: ")
                cnpj = input("Insira o CNPJ do vendedor: ")
                insertVendedor(nome,email,cnpj)
            case '3':
                index = 0
                vendedores = findAllVendedores()      

                index =  int(input("Digite o index do vendedor desejado: "))
                vendedor = vendedores[index]

                nomeProduto = input("Digite o nome do seu produto: ")
                precoProduto = input("Digite o preço do seu produto: ")
                descricaoProduto = input("Digite a descrição do seu produto: ")
                insertProduto(vendedor,nomeProduto, precoProduto, descricaoProduto)
            case '4':
                index = 0
                usuarios = findAllUsuarios()      

                index = int(input("Digite o index do usuário desejado: "))
                usuario = usuarios[index]
                produtos = findAllProdutos()

                listaCompra = []
                listaProdutos = produtos
                comprando = True
                while comprando:      
                    index = 0
                    print("X - Cancelar")
                    print("P - Prosseguir ")
                    
                    index = input("Digite o index do produto desejado: ")
                    if index == 'X':
                        comprando = False
                        break
                    elif index == 'P':
                        if len(listaCompra) == 0 :
                            print("Adicione ao menos um item na sua compra !")
                        else:
                            for compra in listaCompra:
                                insertCompra(usuario,compra["vendedor"],compra) 
                            
                        comprando = False
                        break

                    else:
                        index = int(index)
                        listaCompra.append(listaProdutos[index])
                        del listaProdutos[index]
            case '5':
               print(findUsuario(None, input("Digite o email do usuário: ")))   
            case '6':
                print(findVendedor(None, input("Digite o email do vendedor: ")))   
            case '7':
                print(findProduto(input("Digite o email do vendedor: ")))
            case '8':
                decisao = input("Encontrar as compras por:\n 0 - Usuário \n 1 - Vendedor \n")
                if decisao == '0':
                    print(findCompra(input("Digite o email do usuário: "), None))
                else:
                    print(findCompra(None, input("Digite o email do Vendedor: ")))
            case '9':
                index = 0
                usuarios = findAllUsuarios()      

                index = int(input("Digite o index do usuário desejado: "))
                

                nome = input("Insira o NOME do usuário : ")
                cpf = input("Insira o CPF do usuário: ")
                senha = input("Insira a SENHA do usuário: ")
                print("-----ENDEREÇO-----")
                cep = input("Insira o CEP do usuário: ")
                estado = input("Insira o ESTADO do usuário: ")
                cidade = input("Insira a CIDADE do usuário: ")
                rua = input("Insira a RUA do usuário: ")
                numero = input("Insira o NÚMERO do usuário: ")

                updateUsuario(usuarios[index],nome,cpf,senha,{"cep":cep, "estado": estado, "cidade": cidade, "rua": rua, "numero": numero})

            case '10':
                index = 0
                vendedores = findAllVendedores()      

                index = int(input("Digite o index do vendedor desejado: "))
                nome = input("Insira o NOME do vendedor: ")
                cnpj = input("Insira o CNPJ do vendedor: ")

                updateVendedor(vendedores[index],nome,cnpj)
            case '11':
                index = 0
                produtos = findAllProdutos()
               
                index = int(input("Digite o index do produto desejado: "))
                nomeProduto = input("Digite o nome do seu produto: ")
                precoProduto = input("Digite o preço do seu produto: ")
                descricaoProduto = input("Digite a descrição do seu produto: ")

                updateProduto(produtos[index],nomeProduto,precoProduto,descricaoProduto)
            case '12':
                index = 0
                usuarios = findAllUsuarios()      

                for usuario in usuarios:
                    print(str(index) + ' - ' + usuario.get("nome"))
                    index = index + 1
                index = int(input("Digite o index do usuário desejado: "))

                deleteUsuario(usuarios[index])
            case '13':
                index = 0
                vendedores = findAllVendedores()      

                for vendedor in vendedores:
                    print(str(index) + ' - ' + vendedor.get("nome"))
                    index = index + 1

                index = int(input("Digite o index do vendedor desejado: "))

                deleteVendedor(vendedores[index])
            case '14':
                index = 0
                produtos = findAllProdutos()

                for produto in produtos:
                    print(str(index) + ' - ' + produto.get("nome"))
                    index = index + 1
                index = int(input("Digite o index do produto desejado: "))

                deleteProduto(produtos[index])               
            case '0':
                print("Até a Próxima!")
                loop = False
                break
            case _ :
                print("Operação não entendida")


menu()


