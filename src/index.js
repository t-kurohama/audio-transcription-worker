// src/index.js

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    }
  });
}

async function retryFetch(url, options, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      if (response.status >= 500 && i < maxRetries - 1) {
        await new Promise(r => setTimeout(r, 1000 * (i + 1)));
        continue;
      }
      return response;
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise(r => setTimeout(r, 1000 * (i + 1)));
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type'
        }
      });
    }
    
    if (request.method === 'POST' && url.pathname === '/') {
      return handleUpload(request, env, url);
    }
    
    if (request.method === 'POST' && url.pathname.startsWith('/webhook/')) {
      const jobId = url.pathname.split('/')[2];
      return handleWebhook(request, env, jobId);
    }
    
    return new Response('Not Found', { status: 404 });
  }
};

async function handleUpload(request, env, url) {
  console.log('[INFO] Upload started');
  try {
    const formData = await request.formData();
    const audioFile = formData.get('audio');
    
    if (!audioFile) {
      return jsonResponse({ error: 'No audio file' }, 400);
    }
    
    const jobId = crypto.randomUUID();
    console.log('[INFO] JobId:', jobId);
    
    // R2にアップロード
    const audioFileName = `${jobId}.wav`;
    const arrayBuffer = await audioFile.arrayBuffer();
    
    console.log('[INFO] Uploading to R2...');
    await env.AUDIO_BUCKET.put(audioFileName, arrayBuffer, {
      httpMetadata: {
        contentType: audioFile.type || 'audio/wav'
      }
    });
    
    // 公開URL生成
    const audioUrl = `${env.R2_PUBLIC_URL}/${audioFileName}`;
    console.log('[INFO] Audio URL:', audioUrl);
    
    // RunPod呼び出し
    const webhookUrl = `${url.origin}/webhook/${jobId}`;
    console.log('[INFO] Calling RunPod:', env.RUNPOD_ENDPOINT);
    
    const runpodResponse = await retryFetch(`${env.RUNPOD_ENDPOINT}/run`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.RUNPOD_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        input: {
          audio_url: audioUrl,
          webhook: webhookUrl,
          lang: 'ja'
        }
      })
    });
    
    if (!runpodResponse.ok) {
      const errorText = await runpodResponse.text();
      console.log('[ERROR] RunPod error:', runpodResponse.status, errorText);
      throw new Error(`RunPod error: ${runpodResponse.status}`);
    }
    
    const runpodData = await runpodResponse.json();
    console.log('[SUCCESS] Job started');
    
    return jsonResponse({
      jobId: runpodData.id || jobId,
      message: 'Job started'
    });
    
  } catch (error) {
    console.error('[ERROR]', error.message);
    return jsonResponse({ error: error.message }, 500);
  }
}

async function handleWebhook(request, env, jobId) {
  console.log('[INFO] Webhook:', jobId);
  try {
    const data = await request.json();
    
    if (data.status !== 'COMPLETED') {
      await sendSlack(env, jobId, 'FAILED', null, data.error);
      return new Response('Job failed', { status: 200 });
    }
    
    const fileName = `segments_${jobId}.json`;
    await env.RESULT_BUCKET.put(fileName, JSON.stringify(data.output, null, 2), {
      httpMetadata: { contentType: 'application/json' }
    });
    
    await sendSlack(env, jobId, 'SUCCESS', fileName, null);
    return new Response('OK', { status: 200 });
  } catch (error) {
    console.error('[ERROR]', error.message);
    await sendSlack(env, jobId, 'ERROR', null, error.message);
    return new Response('Error', { status: 500 });
  }
}

async function sendSlack(env, jobId, status, fileName, error) {
  const message = status === 'SUCCESS'
    ? {
        text: '完了',
        blocks: [
          { type: 'header', text: { type: 'plain_text', text: '✅ 完了' } },
          { type: 'section', fields: [
            { type: 'mrkdwn', text: `*ジョブID:*\n\`${jobId}\`` },
            { type: 'mrkdwn', text: `*ファイル:*\n\`${fileName}\`` }
          ]},
          { type: 'section', text: { type: 'mrkdwn', 
            text: `ダウンロード:\n\`\`\`wrangler r2 object get audio-transcription/${fileName} --file ${fileName}\`\`\`` 
          }}
        ]
      }
    : {
        text: '失敗',
        blocks: [
          { type: 'header', text: { type: 'plain_text', text: '❌ 失敗' } },
          { type: 'section', fields: [
            { type: 'mrkdwn', text: `*ジョブID:*\n\`${jobId}\`` },
            { type: 'mrkdwn', text: `*エラー:*\n${error}` }
          ]}
        ]
      };
  
  await fetch(env.SLACK_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(message)
  });
}