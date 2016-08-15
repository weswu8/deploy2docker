#!/bin/sh

`aws ecr get-login`
docker rmi `docker images`

cd gatekeeper
docker build -t gatekeeper .
docker tag gatekeeper:latest 671223302517.dkr.ecr.us-east-1.amazonaws.com/gatekeeper:latest
docker push 671223302517.dkr.ecr.us-east-1.amazonaws.com/gatekeeper:latest

cd ../inventorymanager
docker build -t inventorymanager .
docker tag inventorymanager:latest 671223302517.dkr.ecr.us-east-1.amazonaws.com/inventorymanager:latest 
docker push 671223302517.dkr.ecr.us-east-1.amazonaws.com/inventorymanager:latest

cd ../policycontroller
docker build -t policycontroller .
docker tag policycontroller:latest 671223302517.dkr.ecr.us-east-1.amazonaws.com/policycontroller:latest 
docker push 671223302517.dkr.ecr.us-east-1.amazonaws.com/policycontroller:latest

cd ../safeprotector
docker build -t safeprotector .
docker tag safeprotector:latest 671223302517.dkr.ecr.us-east-1.amazonaws.com/safeprotector:latest 
docker push 671223302517.dkr.ecr.us-east-1.amazonaws.com/safeprotector:latest

cd ../shoppingcart
docker build -t shoppingcart .
docker tag shoppingcart:latest 671223302517.dkr.ecr.us-east-1.amazonaws.com/shoppingcart:latest 
docker push 671223302517.dkr.ecr.us-east-1.amazonaws.com/shoppingcart:latest

cd ..