import asyncio
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import aiosqlite
import aiosmtplib


async def get_contacts(query):
    async with aiosqlite.connect("contacts.db") as db:
        async with db.execute(query) as cursor:
            async for row in cursor:
                yield row


async def send_hello_emails():
    async for record in get_contacts('SELECT * from contacts'):
        content = f"""
            <body>
                <h2 style='text-align: center'>Уважаемый {record[1]}!</h2>
                <p>Спасибо, что пользуетесь нашим сервисом объявлений.</p>
            </body>
        """

        message = MIMEMultipart()
        message["To"] = record[3]
        message['From'] = '***'
        message.attach(MIMEText(content, 'html'))

        server = aiosmtplib.SMTP(hostname='smtp.rambler.ru', port='465')
        await server.connect()
        await server.starttls()
        await server.login(username='***', password='***')
        await server.sendmail(message['From'], message["To"], message.as_string())
        server.close()

asyncio.run(send_hello_emails())
