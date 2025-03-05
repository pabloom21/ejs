'''
Listar todos los tickets asignados a los distintos usuarios que tienen Asignado a
'''
import psycopg2
import pandas
import config as CF

def conectar(cf): #La variable es el fichero en el que están los datos de conexión
	con = psycopg2.connect(database = cf.DATABASE, user = cf.USER, host = cf.HOST, password = cf.PASSWORD, port = cf.PORT)
	return con
def desconectar(conn):
	conn.close()

def tickets(user, conn): # Devuelve los tickets de un determinado usuario
	cur = conn.cursor()
	consulta = f"""
	select
		a.glpi_instance_name,
   		b.group_name, 
		a.ticket_name, 
		a.ticket_user_id_vinculation_type,
		a.ticket_link
	from 
		glpi_tickets_tasks_projects_202502240956 a
	left join
		glpi_users_groups_202502240956 b 
	on 
		(a.glpi_instance_name,a.ticket_user_id_vinculation) = (b.glpi_instance_name,b.user_id)
	where 
		b.user_name = '{user}' and ticket_state_status != 'Finalizado' 
	GROUP BY 
		a.glpi_instance_name, 
		b.group_name, 
		a.ticket_name, 
		a.ticket_user_id_vinculation_type, 
		a.ticket_link;
	"""
	tabla = pandas.read_sql(consulta, conn)
	cur.close()
	return tabla

def users(conn): # Devuelve una lista con los usuarios que se encuentran en la tabla de tickets
	cur = conn.cursor()
	consulta = """
	select distinct
		b.user_name
	from 
		glpi_tickets_tasks_projects_202502240956 a
	left join
		glpi_users_groups_202502240956 b
	on 
		(a.glpi_instance_name,a.ticket_user_id_vinculation) = (b.glpi_instance_name,b.user_id)
	"""
	tabla = pandas.read_sql(consulta, conn)
	cur.close()
	lista = []
	for i in tabla.user_name:
		lista.append(i)
	return lista

def main():
	conn = conectar(CF)
	usuarios = users(conn)
	#usuarios = ['jgonzal']
	for usuario in usuarios:
		o = 0
		ti = tickets(usuario, conn)
		if ti.empty != True:
			t = []
			for i in ti.ticket_name:
				if i:
					t.append(i)
				else:
					t.append('None')
			enlace = []
			for j in ti.ticket_link:
				if j:
					enlace.append(j)
				else:
					enlace.append('None')
			instancia = list()
			grupo = list()
			for k in ti.glpi_instance_name:
				if k:
					instancia.append(k)
				else:
					instancia.append('None')
			for m in ti.group_name:
				if m:
					grupo.append(m)
				else:
					grupo.append('None')

			fin = []
			c = 0
			for x in ti.ticket_user_id_vinculation_type:
				if x == 'Asignada a':
					enl = f'<a href={enlace[c]}>{x}: {t[c]}</a>'
					fin.append(enl)
				c += 1
			f = open(f'{usuario}.html', 'w', encoding='utf-8')
			f.write(f'Tickets del usuario {usuario}: \n')
			for j in fin:
				f.write(f'{instancia[o]}, {grupo[o]}, {j}  \n')
				if o != len(instancia)-1:
					if instancia[o] != instancia[o+1]:
						f.write('\n')
					if grupo[o] != grupo[o+1]:
						f.write('\n')
				o += 1
			f.close()
	desconectar(conn)
 
if __name__ == '__main__':
	main()


