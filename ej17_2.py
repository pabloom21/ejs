'''
Listar todos los tickets asignados a jgonzal
'''
import psycopg2
import pandas
import config as CF
conn = psycopg2.connect(database = CF.DATABASE, user = CF.USER, host = CF.HOST, password = CF.PASSWORD, port = CF.PORT)
def tickets(user): # Devuelve los tickets de un determinado usuario
	cur = conn.cursor()
	consulta = f"""
	select
		a.ticket_name, 
		a.ticket_user_id_vinculation_type
	from 
		glpi_tickets_tasks_projects_202502240956 a
	left join
		glpi_users_groups_202502240956 b 
	on 
		(a.glpi_instance_name,a.ticket_user_id_vinculation) = (b.glpi_instance_name,b.user_id)
	where 
		b.user_name = '{user}' and ticket_state_status != 'Finalizado'
	GROUP BY 
		a.ticket_name, a.ticket_user_id_vinculation_type;
	"""
	tabla = pandas.read_sql(consulta, conn)
	cur.close()

	return tabla

def users(): # Devuelve una lista con los usuarios que se encuentran en la tabla de tickets
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
	usuarios = ['jgonzal']
	for usuario in usuarios:
		ti = tickets(usuario)
		if ti.empty != True:
			t = []
			for i in ti.ticket_name:
				if i:
					t.append(i)
				else:
					t.append('None')
			fin = []
			c = 0
			for x in ti.ticket_user_id_vinculation_type:
				if x == 'Asignada a':
					fin.append(t[c] + f', {x}')
				c += 1
			f = open(f'{usuario}_2.html', 'w')
			f.write(f'Tickets del usuario {usuario}: \n')
			for j in fin:
				f.write(f'{j}\n')
			f.close()
 
if __name__ == '__main__':
	main()
conn.close()
