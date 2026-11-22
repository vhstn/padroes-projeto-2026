# Seminário (Individual) – Padrões de Projeto de Integração, Microsserviços e Mensageria

**Tema**: 07 - Apache Kafka (https://kafka.apache.org/)

---

## 1. Definição

O **Apache Kafka** é uma plataforma de código aberto criada para enviar, armazenar e processar grandes volumes de mensagens e eventos em tempo real, com alta velocidade e resistência a falhas.

Diferente de filas tradicionais, que apagam a mensagem logo após a entrega, o Kafka guarda os registros em disco por um tempo configurável. Isso permite que vários sistemas leiam os mesmos dados de forma independente e no seu próprio ritmo.

---

## 2. Histórico, Data de Criação e Responsáveis

- **Origem e Data de Criação**: Criado no **LinkedIn** entre 2010 e **2011** pelos engenheiros Jay Kreps, Neha Narkhede e Jun Rao, para centralizar o envio de métricas, registros de uso e a comunicação entre sistemas internos.
- **Código Aberto**: Liberado no GitHub em janeiro de 2011 e doado à **Apache Software Foundation** no mesmo ano, tornando-se um projeto principal da fundação em outubro de 2012.
- **Empresa Associada**: Em 2014, os criadores do Kafka fundaram a **Confluent**, empresa que hoje oferece suporte comercial e ferramentas ligadas à tecnologia.

---

## 3. Funcionamento

O Kafka funciona com servidores chamados **brokers**, que recebem e guardam as mensagens. Essas mensagens são organizadas em canais chamados **tópicos**, que podem ser divididos em **partições** para distribuir o trabalho entre vários servidores. Dentro de cada partição, cada mensagem recebe um número sequencial, chamado **offset**, que marca sua posição e garante a ordem de leitura.

As aplicações que enviam dados são os **produtores**, e as que leem são os **consumidores**. Quando vários consumidores trabalham juntos em um grupo, cada um fica responsável por parte das partições para dividir a carga, sem apagar as mensagens do servidor. Isso permite que outros sistemas também leiam o mesmo histórico sempre que precisarem.

---

## 4. Principais Aplicações

1. **Comunicação entre microsserviços**: Um serviço avisa que algo aconteceu, como um novo pedido, e os demais serviços de estoque, pagamento e notificação reagem de forma independente.
2. **Processamento em tempo real**: Acompanhamento imediato de compras, cliques em sites, detecção de fraudes e recomendações.
3. **Sincronização de bancos de dados**: Captura automática de alterações no banco para atualizar outros sistemas, relatórios ou buscas.
4. **Centralização de registros e métricas**: Coleta em grande escala de logs de servidores, dados de sensores e monitoramento de aplicações.
5. **Histórico de eventos**: Registro completo de tudo o que ocorreu em um sistema, permitindo reconstruir estados anteriores quando necessário.