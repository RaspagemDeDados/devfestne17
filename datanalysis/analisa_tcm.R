library(data.table)
library(tidyverse)

#setting working directory
setwd("/home/cdesantana/DataSCOUT/TCM")

#reading the dataset (using 'data.table::fread' function because it reads data faster than other functions)
dat <- fread("./tcm.csv",sep=",",header=T)

#setting "teto salarial"
teto <- 39293.32 

#subset based on the maximum 'Salário Base' of each 'Município'
subdat <- dat %>% select(Município, `Salário Base`) %>% group_by(Município) %>% summarise(max_sal = max(`Salário Base`)) %>% arrange(max_sal) %>% tail(50)

#defining the plot
p <- ggplot(subdat[-c(47,48,49,50),]) + 
   geom_bar(stat="identity", aes(x = toupper(reorder(Município, as.numeric(max_sal))), y=as.numeric(max_sal))) + ylab("Número de supersalários") + xlab("Cidade") + coord_flip()

#plotting the png figure
png("lista_maiores_salarios.png",width=3200,height=1800,res=300)
print(p)
dev.off()

#####

#upper the names of the 'Município's
dat$Município <- toupper(dat$Município)

#counting for the number of 'Salario Base' that are higher than 'teto' in each 'Município'
subdat <- dat %>% filter(`Salário Base` > teto) %>% group_by(Município) %>% summarise(nsupersalarios = n()) %>% arrange(nsupersalarios) %>% tail(100)

#defining the plot
p <- ggplot(subdat) + 
   geom_bar(stat="identity", aes(x = reorder(Município,as.numeric(nsupersalarios)), y=as.numeric(nsupersalarios))) + ylab("Número de supersalários") + xlab("Cidade") + coord_flip()

#plotting the png figure
png("cidades_mais_supersalarios.png",width=3200,height=1800,res=300)
print(p)
dev.off()


#####

dat$Cargo <- toupper(dat$Cargo)

#counting for the number of 'Salario Base' that are higher than 'teto' in each 'Cargo'
subdat <- dat %>% filter(`Salário Base` > teto) %>% group_by(Cargo) %>% summarise(nsupersalarios = n()) %>% arrange(nsupersalarios) %>% tail(100)

p <- ggplot(subdat) + 
   geom_bar(stat="identity", aes(x = reorder(Cargo,as.numeric(nsupersalarios)), y=as.numeric(nsupersalarios))) + ylab("Número de supersalários") + xlab("Cargos") + coord_flip()

png("cargos_mais_supersalarios.png",width=3200,height=1800,res=300)
print(p)
dev.off()

#####

dat$`Tipo Servidor` <- toupper(dat$`Tipo Servidor`)

#counting for the number of 'Salario Base' that are higher than 'teto' in each 'Tipo Servidor'
subdat <- dat %>% filter(`Salário Base` > teto) %>% group_by(`Tipo Servidor`) %>% summarise(nsupersalarios = n()) %>% arrange(nsupersalarios) %>% tail(100)

p <- ggplot(subdat) + 
   geom_bar(stat="identity", aes(x = reorder(`Tipo Servidor`,as.numeric(nsupersalarios)), y=as.numeric(nsupersalarios))) + ylab("Tipo de Servidor") + xlab("Cidade") + coord_flip()

png("tiposervidor_supersalarios.png",width=3200,height=1800,res=300)
print(p)
dev.off()

