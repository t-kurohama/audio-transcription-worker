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
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type'
        }
      });
    }
    
    if (request.method === 'POST' && url.pathname === '/') {
      return handleUpload(request, env, url);
    }
    
    if (request.method === 'GET' && url.pathname.startsWith('/download/')) {
      const fileName = url.pathname.split('/').slice(2).join('/');
      return handleDownload(request, env, fileName);
    }
    
    if (request.method === 'GET' && url.pathname.startsWith('/download-result/')) {
      const filePath = url.pathname.substring(17);
      return handleDownloadResult(request, env, filePath);
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
    const client = formData.get('client');
    const vid = formData.get('vid');
    
    if (!audioFile) {
      return jsonResponse({ error: 'No audio file' }, 400);
    }
    
    if (!client || !vid) {
      return jsonResponse({ error: 'client and vid are required' }, 400);
    }
    
    const jobId = crypto.randomUUID();
    console.log('[INFO] JobId:', jobId);
    console.log('[INFO] Client:', client, 'Vid:', vid);
    
    // R2にアップロード（パスを動的に）
    const audioFileName = `projects/${client}/${vid}/${jobId}.wav`;
    const arrayBuffer = await audioFile.arrayBuffer();
    
    console.log('[INFO] Uploading to R2...');
    await env.AUDIO_BUCKET.put(audioFileName, arrayBuffer, {
      httpMetadata: {
        contentType: audioFile.type || 'audio/wav'
      }
    });
    console.log('[INFO] R2 upload done');
    
    // Workers経由のダウンロードURL生成
    const audioUrl = `${url.origin}/download/${audioFileName}`;
    console.log('[INFO] Download URL:', audioUrl);
    
    // RunPod呼び出し
    const webhookUrl = `${url.origin}/webhook/${jobId}`;
    console.log('[INFO] Calling RunPod');
    
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
          lang: 'ja',
          client: client,
          vid: vid
        }
      })
    });
    
    if (!runpodResponse.ok) {
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

async function handleDownload(request, env, fileName) {
  console.log('[INFO] Download request:', fileName);
  try {
    const object = await env.AUDIO_BUCKET.get(fileName);
    
    if (!object) {
      console.error('[ERROR] File not found:', fileName);
      return new Response('File not found', { status: 404 });
    }
    
    console.log('[INFO] Serving file:', fileName);
    return new Response(object.body, {
      headers: {
        'Content-Type': object.httpMetadata?.contentType || 'audio/wav',
        'Cache-Control': 'public, max-age=3600',
        'Access-Control-Allow-Origin': '*'
      }
    });
  } catch (error) {
    console.error('[ERROR]', error.message);
    return new Response('Error', { status: 500 });
  }
}

async function handleDownloadResult(request, env, filePath) {
  console.log('[INFO] Download result request:', filePath);
  try {
    const object = await env.RESULT_BUCKET.get(filePath);
    
    if (!object) {
      console.error('[ERROR] File not found:', filePath);
      return new Response('File not found', { status: 404 });
    }
    
    console.log('[INFO] Serving result file:', filePath);
    return new Response(object.body, {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600',
        'Access-Control-Allow-Origin': '*'
      }
    });
  } catch (error) {
    console.error('[ERROR]', error.message);
    return new Response('Error', { status: 500 });
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
    
    // client/vidを取得（RunPodから返される）
    const client = data.input?.client || 'unknown';
    const vid = data.input?.vid || 'unknown';
    
    const fileName = `segments_${jobId}.json`;
    const filePath = `projects/${client}/${vid}/${fileName}`;
    
    await env.RESULT_BUCKET.put(filePath, JSON.stringify(data.output, null, 2), {
      httpMetadata: { contentType: 'application/json' }
    });
    
    await sendSlack(env, jobId, 'SUCCESS', filePath, null);
    
    // Apps Script呼び出し
    if (env.APPS_SCRIPT_URL) {
      try {
        const signedUrl = `https://flat-paper-c3c1.throbbing-shadow-24bc.workers.dev/download-result/${filePath}`;
        
        const gasResponse = await fetch(env.APPS_SCRIPT_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            r2_url: signedUrl,
            file_name: 'segments.json',
            client: client,
            vid: vid
          })
        });
        
        const gasResult = await gasResponse.json();
        
        if (gasResult.success) {
          await sendSlack(env, jobId, 'DRIVE_SUCCESS', gasResult.driveUrl, null);
        } else {
          await sendSlack(env, jobId, 'DRIVE_FAILED', null, gasResult.error);
        }
        
      } catch (error) {
        console.error('[ERROR] Apps Script call failed:', error);
        await sendSlack(env, jobId, 'DRIVE_FAILED', null, error.message);
      }
    }
    
    return new Response('OK', { status: 200 });
  } catch (error) {
    console.error('[ERROR]', error.message);
    await sendSlack(env, jobId, 'ERROR', null, error.message);
    return new Response('Error', { status: 500 });
  }
}

async function sendSlack(env, jobId, status, fileNameOrUrl, error) {
  let message;
  
  if (status === 'SUCCESS') {
    message = {
      text: '完了',
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: '✅ 完了' } },
        { type: 'section', fields: [
          { type: 'mrkdwn', text: `*ジョブID:*\n\`${jobId}\`` },
          { type: 'mrkdwn', text: `*ファイル:*\n\`${fileNameOrUrl}\`` }
        ]},
        { type: 'section', text: { type: 'mrkdwn', 
          text: `ダウンロード:\n\`\`\`wrangler r2 object get audio-transcription/${fileNameOrUrl} --file result.json --remote\`\`\`` 
        }}
      ]
    };
  } else if (status === 'DRIVE_SUCCESS') {
    message = {
      text: 'Drive保存完了',
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: '📁 Drive保存完了' } },
        { type: 'section', text: { type: 'mrkdwn', text: `${fileNameOrUrl}` }}
      ]
    };
  } else if (status === 'DRIVE_FAILED') {
    message = {
      text: 'Drive保存失敗',
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: '⚠️ Drive保存失敗' } },
        { type: 'section', text: { type: 'mrkdwn', text: `エラー: ${error}` }}
      ]
    };
  } else {
    message = {
      text: '失敗',
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: '❌ 失敗' } },
        { type: 'section', fields: [
          { type: 'mrkdwn', text: `*ジョブID:*\n\`${jobId}\`` },
          { type: 'mrkdwn', text: `*エラー:*\n${error}` }
        ]}
      ]
    };
  }
  
  await fetch(env.SLACK_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(message)
  });
}
