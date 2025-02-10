'''
Listar todos los tickets de un grupo, por ejemplo el grupo 'GrupoDirecciónTIC' y que me de para cada ticket
si tiene un usuario asignado o no y el tipo de asignación del usuario
'''
import psycopg2
import pandas
conn = psycopg2.connect(database = 'postgres', user = 'postgres', host = 'localhost', password = 'python', port = 5432)
cur = conn.cursor()
consulta = '''
select distinct
	a.ticket_name as ticket,
	a.ticket_user_id_vinculation_type as tipo_de_asignación,
	a.ticket_user_id_vinculation as id_usuario_vinculado
from 
	glpi_tickets_tasks_projects_202501301345 a
left join
	glpi_users_groups_202501301353 b
on
	a.ticket_group_id_vinculation = b.group_id
where
	b.group_name = 'GrupoDirecciónTIC' 
and 
	b.user_name = 'jgonzal' 
and 
	ticket_state_status != 'Finalizado'
'''
tabla = pandas.read_sql(consulta, conn)
print(tabla)
cur.close()
conn.close()