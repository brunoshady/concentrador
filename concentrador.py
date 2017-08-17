import os
import psycopg2
import time
import datetime
import os.path
import random

motdbool,user,password,host,port,dbname = False,'','','','',''

def motd():
    global motdbool,user,password,host,port,dbname 
    user = os.getenv('PGUSER')
    if user is None:
        user = 'postgres'
    password = os.getenv('PGPASSWORD')
    if password is None:
        password = 'postgres'
    host = os.getenv('PGHOST')
    if host is None:
        host = 'localhost'
    port = os.getenv('PGPORT')
    if port is None:
        port = '5432'
    dbname = os.getenv('PGDATABASE')
    if dbname is None:
        dbname = 'autosystem'
        
    if motdbool == False:
        motdbool = True
        log("")
        log("Utilizando configuracoes padroes, para alterar, modifique as variaveis de ambiente.")
        log("PGUSER=usuario do banco")
        log("PGPASSWORD=senha do banco")
        log("PGHOST=servidor")
        log("PGPORT=porta do servidor")
        log("PGDATABASE=nome do banco")
        log("")
        log("Conectando banco de dados '%s' em '%s' user '%s'" % (dbname, host, user))

def log(msg):
    if not os.path.isfile('C:\\autosystem\log\concentrador.log'):
        file = open('C:\\autosystem\log\concentrador.log', 'w+')  
    with open('C:\\autosystem\log\concentrador.log', 'a') as file:
        file.write('*** ' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + ': ' + msg + '\n')


def connPostgreSQL():
    try:   
        conn = psycopg2.connect("user='%s' password='%s' host='%s' port='%s' dbname='%s' " % (user,password,host,port,dbname))
    except (Exception, psycopg2.DatabaseError) as error:
        log(str(error))
    return conn


def closePostreSQL(self):
    try:
        self.close()
    except (Exception, psycopg2.DatabaseError) as error:
        log(str(error))    
    

def updateSQL(query):
    conn = connPostgreSQL()
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        log(str(error))
    closePostreSQL(conn)


def selectSQL(query):
    conn = connPostgreSQL()
    cur = conn.cursor()
    try:
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        log(str(error))
    rows = cur.fetchall()
    closePostreSQL(conn)    
    return rows

    
def main():
    motd()
    log("Atualizando bicos para posicao 'L'")
    updateSQL("UPDATE bico_status SET status = 'L'")
    
    log("Buscando quantidade de abastecimentos...")
    rows = selectSQL("SELECT count(*) FROM abastecimento")
    for row in rows:
        qtdeAbastecimentos = row[0]

    if qtdeAbastecimentos < 50:
        rows = selectSQL("""SELECT bb.bomba, p.preco_unit
                            FROM bico_bomba bb
                            JOIN bico b ON (b.nome = bb.bico)
                            JOIN deposito d ON (b.deposito = d.grid)
                            JOIN produto p ON (p.grid = d.produto)
                            ORDER BY RANDOM() LIMIT 1""")
        for row in rows:
            bico = row[0]
            preco_unit = row[1]

        rows = selectSQL("""SELECT pi.pessoa, p.nome FROM pessoa_id pi
                            JOIN identificacao i ON (pi.tipo_id = i.grid)
                            JOIN pessoa p ON (p.grid = pi.pessoa)
                            WHERE i.nome = 'RFID' ORDER BY RANDOM() LIMIT 1""")

        if len(rows) == 0:
            frentista = 'NULL'
            nomefrentista = ''
        else:
            for row in rows:
                frentista = row[0]
                nomefrentista = row[1]   
                     
        valores = [5,10,20,30,40,50,60,70,80,90,100,150,200]
        valor = random.choice(valores)

        quantidade = (valor/preco_unit)

        log("Setando o bico %s para modo 'A'..." % bico)
        updateSQL("UPDATE bico_status SET status = 'A' WHERE bico = '%s'" % bico)
        
        time.sleep(quantidade/2)

        if frentista == 'NULL':
            log("Inserindo abastecimento no bico %s no valor de %.3f, %.3f litros..." % (bico, valor, quantidade))
        else:
            log("Inserindo abastecimento no bico %s no valor de %.3f, %.3f litros para o frentista '%s'..." % (bico, valor, quantidade, nomefrentista))

        updateSQL("""INSERT INTO abastecimento (bico, quantidade, preco_unit, valor, hora, frentista) VALUES
                     ('%s','%.3f','%.3f','%.3f',NOW(),%s)""" % (bico, quantidade, preco_unit, valor, frentista))

        log("Abastecimento inserido, aguardando 15 segundos")
        time.sleep(15)
    else:
        log("Encontrado %s abastecimentos, aguardando 120 segundos..." % qtdeAbastecimentos)
        time.sleep(120)
        
    main()

if __name__ == '__main__':
    main()
        
