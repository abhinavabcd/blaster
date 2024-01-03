sudo docker network create blaster-test-mongo-network
sudo docker stop blaster-test-mongo-1 || true && sudo docker rm -f blaster-test-mongo-1 && sudo docker run --name blaster-test-mongo-1 --hostname blaster-test-mongo-1 --net blaster-test-mongo-network -d -p 9042:9042 mongo:5.0.1-focal mongod --port=9042 --replSet=rs1
sudo docker stop blaster-test-mongo-2 || true && sudo docker rm -f blaster-test-mongo-2 && sudo docker run --name blaster-test-mongo-2 --hostname blaster-test-mongo-2 --net blaster-test-mongo-network -d -p 9142:9142 mongo:5.0.1-focal mongod --port=9142 --replSet=rs1
sudo docker stop blaster-test-mongo-3 || true && sudo docker rm -f blaster-test-mongo-3 && sudo docker run --name blaster-test-mongo-3 --hostname blaster-test-mongo-3 --net blaster-test-mongo-network -d -p 9242:9242 mongo:5.0.1-focal mongod --port=9242 --replSet=rs1

comamnd='rs.initiate({"_id" : "rs1", "members" : [{"_id" : 0, "host" :"blaster-test-mongo-1:9042"}, {"_id" : 1, "host": "blaster-test-mongo-2:9142"}, {"_id" : 2,"host": "blaster-test-mongo-3:9242"}]});rs.config();'
echo $comamnd | sudo docker exec -i blaster-test-mongo-1 mongosh --port 9042

IS_TEST=1 python3 -m unittest $1 $2
