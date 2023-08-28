select T.Instancia,T.{id},T.{text},T.{interlocutor},T.{dtText},I.{dtSTT}         
from Interaction I  
inner join Transcription T on I.InteractionId = T.{id} and I.Instancia = T.Instancia
where I.{dtSTT} >= '{ini}' and I.{dtSTT} < '{end}' and I.Instancia = {instancias} limit 50000000
