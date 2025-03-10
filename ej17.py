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
		ti = tickets(usuario, conn)
		if ti.empty != True:
			enlaces = list()
			c = 0
			truelist = list()
			for x in ti.ticket_link:
				if x:
					enl = f'<a href={ti.ticket_link[c]}> {ti.ticket_name[c]}</a>'
					enlaces.append(enl)
				else:
					enlaces.append('None')
				c += 1
			for d in range(len(enlaces)):
				if enlaces[d] != 'None' and ti.ticket_user_id_vinculation_type[d] == 'Asignada a':
					truelist.append(d)
			f = open(f'{usuario}.html', 'w', encoding='utf-8')
			f.write(f'Tickets del usuario {usuario}: \n<br>')
			for h in range(len(truelist)):
				ind = truelist[h]
				ind_1 = truelist[h-1]
				if h == 0:
					f.write(f'- {ti.glpi_instance_name[ind]}\n<br>')
					f.write(f'<p style="text-indent: 30px;">* {ti.group_name[ind]}\n<br>')
				if h != 0 and ti.glpi_instance_name[ind] != ti.glpi_instance_name[ind_1]:
					f.write('\n<br>')
					f.write(f'- {ti.glpi_instance_name[ind]}\n<br>')
				if h != 0 and ti.group_name[ind] != ti.group_name[ind_1]:
					f.write('\n<br>')
					print(ti.group_name[ind])
					f.write(f'<p style="text-indent: 30px;">* {ti.group_name[ind]}\n<br>')
				f.write(f'<p style="text-indent: 30px;">{enlaces[h]}  \n<br>')
			f.close()

	desconectar(conn)
 
if __name__ == '__main__':
	main()



