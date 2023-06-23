from blaster.tools import get_random_id, cur_ms
from blaster.config import IS_DEV
from blaster.mongo_orm import Connection, Model, Attribute, init_mongo_cluster


#step 1
class SalesAgent(Model):
	_collection_name_ = "domain_agents"

	company = Attribute(str)
	agent_user_id = Attribute(str)
	auth_level = Attribute(int, default=1)
	load = Attribute(int, default=0)
	data = Attribute(dict)
	created_at = Attribute(int , default=cur_ms)
	updated_at = Attribute(int, default=cur_ms)

	def before_update(self):
		self.updated_at = cur_ms()

	_index_ = [
				(company, agent_user_id),
				(company, load, {"unique": False})
			]

#call this function after you import all mongo db classes/models
def initialize_mongo():
	nodes = []
	if IS_DEV:
		nodes.append(Connection(host="localhost", port=27017, db_name="xyz"))
	else:
		nodes.append(Connection(host="mongodb://mongo-0.mongo.default.svc/", port=27017, db_name="xyz"))

	#check if connection exists
	nodes[0].db.client.server_info()

	init_mongo_cluster(nodes)


if __name__ == "__main__":
	initialize_mongo()

	for i in range(10):
		sales_agent = SalesAgent(company="example", agent_user_id=get_random_id(5), data={}, load=i).commit()


	sales_agent.data["says"] = "hello"
	sales_agent.commit()
	
	sales_agent.auth_level = 2
	sales_agent.commit()

	print(sales_agent.to_dict())

	#query single item only
	sales_agent = SalesAgent.get(company="example")
	print(sales_agent.to_dict())

	#direct mongo query
	sales_agents = SalesAgent.query({"company": "example", "agent_user_id": {"$in": ["abcd", "pqrs"]}})
	#wont have any
	for i in sales_agents:
		print(i.to_dict())

	#this rreturns a map
	sales_agents = SalesAgent.query({"company": "example"})
	for i in sales_agents:
		print(i.to_dict())
