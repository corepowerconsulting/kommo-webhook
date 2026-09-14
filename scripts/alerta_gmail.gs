/**
 * Alertas de PULSE por Gmail.
 *
 * El servidor de PULSE (Render) avisa cuando Kommo apaga el webhook de una
 * cuenta. No puede mandar el correo por SMTP porque Render bloquea esos puertos
 * en los servicios gratuitos, asi que le pide a este script que lo mande desde
 * la cuenta de Google de quien lo publica.
 *
 * Como instalarlo (una sola vez):
 *   1. https://script.google.com -> Nuevo proyecto. Pegar este archivo entero.
 *   2. Reemplazar PEGAR_AQUI_ALERTA_SECRETO por el valor de ALERTA_SECRETO
 *      (esta en .env.render, en la carpeta del proyecto).
 *   3. Elegir la funcion "probar" arriba y apretar Ejecutar. Google pide
 *      permiso para enviar correos: aceptar. Llega un correo de prueba.
 *   4. Implementar -> Nueva implementacion -> tipo "Aplicacion web".
 *      Ejecutar como: Yo.  Quien tiene acceso: Cualquier usuario.
 *   5. Copiar la URL que termina en /exec y cargarla en Render como
 *      ALERTA_GMAIL_URL.
 *
 * "Cualquier usuario" es necesario para que Render pueda llamarlo, y por eso
 * existe el secreto: sin el, el script no manda nada.
 */
const SECRETO = 'PEGAR_AQUI_ALERTA_SECRETO';

function doPost(e) {
  let d;
  try {
    d = JSON.parse(e.postData.contents);
  } catch (err) {
    return salida('json invalido');
  }
  if (!d || SECRETO === 'PEGAR_AQUI_ALERTA_SECRETO' || d.secreto !== SECRETO) {
    return salida('no autorizado');
  }
  // Sin destinatario explicito, le llega al dueño del script.
  const para = d.para || Session.getEffectiveUser().getEmail();
  MailApp.sendEmail({
    to: para,
    subject: String(d.asunto || 'PULSE'),
    body: String(d.cuerpo || ''),
  });
  return salida('ok');
}

function salida(texto) {
  return ContentService.createTextOutput(texto).setMimeType(ContentService.MimeType.TEXT);
}

// Ejecutar a mano desde el editor la primera vez: es lo que hace que Google
// pida el permiso de enviar correos.
function probar() {
  MailApp.sendEmail(Session.getEffectiveUser().getEmail(),
                    'PULSE: el script de alertas tiene permiso para enviar',
                    'Listo. Falta publicarlo como aplicacion web (paso 4).');
}
