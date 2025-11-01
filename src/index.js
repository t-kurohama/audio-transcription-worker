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
    const numSpeakers = formData.get('num_speakers');
    
    if (!audioFile) {
      return jsonResponse({ error: 'No audio file' }, 400);
    }
    
    if (!client || !vid) {
      return jsonResponse({ error: 'client and vid are required' }, 400);
    }
    
    if (!numSpeakers) {
      return jsonResponse({ error: 'num_speakers is required' }, 400);
    }
    
    const jobId = crypto.randomUUID();
    console.log('[INFO] JobId:', jobId);
    console.log('[INFO] Client:', client, 'Vid:', vid, 'Speakers:', numSpeakers);
    
    const audioFileName = `projects/${client}/${vid}/${jobId}.wav`;
    const arrayBuffer = await audioFile.arrayBuffer();
    
    console.log('[INFO] Uploading to R2...');
    await env.AUDIO_BUCKET.put(audioFileName, arrayBuffer, {
      httpMetadata: {
        contentType: audioFile.type || 'audio/wav'
      }
    });
    console.log('[INFO] R2 upload done');
    
    const audioUrl = `${url.origin}/download/${audioFileName}`;
    console.log('[INFO] Download URL:', audioUrl);
    
    const webhookUrl = `${url.origin}/webhook/${jobId}`;
    console.log('[INFO] Calling Replicate API - WhisperX');
    
    // WhisperX実行
    const whisperxResponse = await retryFetch('https://api.replicate.com/v1/predictions', {
      method: 'POST',
      headers: {
        'Authorization': `Token ${env.REPLICATE_API_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        version: 'd56a8a6bf25c1e17f476e2f3daa8b7c2e4f4dbe757ef5c8b504785e5112c76bb',
        input: {
          audio: audioUrl,
          language: 'ja',
          batch_size: 24,
          diarization: false
        },
        webhook: `${webhookUrl}?type=whisperx`,
        webhook_events_filter: ['completed']
      })
    });
    
    if (!whisperxResponse.ok) {
      const errorText = await whisperxResponse.text();
      throw new Error(`Replicate WhisperX error: ${whisperxResponse.status} - ${errorText}`);
    }
    
    const whisperxData = await whisperxResponse.json();
    console.log('[INFO] WhisperX job started:', whisperxData.id);
    
    // Pyannote実行
    console.log('[INFO] Calling Replicate API - Pyannote');
    const pyannoteResponse = await retryFetch('https://api.replicate.com/v1/predictions', {
      method: 'POST',
      headers: {
        'Authorization': `Token ${env.REPLICATE_API_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        version: '1c597f468b34e7d3a8ddec528d1d39059b12b6b7a2cd7b31b77c8cf257a9e022',
        input: {
          audio: audioUrl,
          min_speakers: Math.max(1, parseInt(numSpeakers) - 1),
          max_speakers: parseInt(numSpeakers) + 2
        },
        webhook: `${webhookUrl}?type=pyannote`,
        webhook_events_filter: ['completed']
      })
    });
    
    if (!pyannoteResponse.ok) {
      const errorText = await pyannoteResponse.text();
      throw new Error(`Replicate Pyannote error: ${pyannoteResponse.status} - ${errorText}`);
    }
    
    const pyannoteData = await pyannoteResponse.json();
    console.log('[INFO] Pyannote job started:', pyannoteData.id);
    
    // ジョブIDを保存（両方のIDを記録）
    await env.AUDIO_BUCKET.put(`jobs/${jobId}.json`, JSON.stringify({
      jobId: jobId,
      whisperxId: whisperxData.id,
      pyannoteId: pyannoteData.id,
      client: client,
      vid: vid,
      numSpeakers: numSpeakers,
      createdAt: new Date().toISOString()
    }), {
      httpMetadata: { contentType: 'application/json' }
    });
    
    console.log('[SUCCESS] Both jobs started');
    
    return jsonResponse({
      jobId: jobId,
      whisperxId: whisperxData.id,
      pyannoteId: pyannoteData.id,
      message: 'Jobs started'
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
  const url = new URL(request.url);
  const type = url.searchParams.get('type'); // whisperx or pyannote
  
  console.log('[INFO] Webhook:', jobId, 'Type:', type);
  
  try {
    const data = await request.json();
    
    if (data.status !== 'succeeded') {
      await sendSlack(env, jobId, 'FAILED', null, `${type} failed: ${data.error}`);
      return new Response('Job failed', { status: 200 });
    }
    
    // ジョブ情報を取得
    const jobInfoObj = await env.AUDIO_BUCKET.get(`jobs/${jobId}.json`);
    if (!jobInfoObj) {
      console.error('[ERROR] Job info not found:', jobId);
      return new Response('Job info not found', { status: 404 });
    }
    
    const jobInfo = JSON.parse(await jobInfoObj.text());
    const client = jobInfo.client;
    const vid = jobInfo.vid;
    
    // 結果を保存
    const fileName = `${type}_${jobId}.json`;
    const filePath = `projects/${client}/${vid}/${fileName}`;
    
    await env.RESULT_BUCKET.put(filePath, JSON.stringify(data.output, null, 2), {
      httpMetadata: { contentType: 'application/json' }
    });
    
    console.log('[INFO] Saved result:', filePath);
    
    // 両方完了したかチェック
    const whisperxPath = `projects/${client}/${vid}/whisperx_${jobId}.json`;
    const pyannnotePath = `projects/${client}/${vid}/pyannote_${jobId}.json`;
    
    const whisperxExists = await env.RESULT_BUCKET.head(whisperxPath);
    const pyannoteExists = await env.RESULT_BUCKET.head(pyannnotePath);
    
    if (whisperxExists && pyannoteExists) {
      console.log('[INFO] Both jobs completed!');
      await sendSlack(env, jobId, 'SUCCESS', filePath, null);
      
      // Google Drive連携（オプション）
      if (env.APPS_SCRIPT_URL) {
        console.log('[INFO] Calling Google Apps Script...');
        
        try {
          const signedUrl = `https://flat-paper-c3c1.throbbing-shadow-24bc.workers.dev/download-result/${filePath}`;
          
          const gasResponse = await fetch(env.APPS_SCRIPT_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              r2_url: signedUrl,
              file_name: fileName,
              client: client,
              vid: vid
            })
          });
          
          const gasResult = await gasResponse.json();
          
          if (gasResult.success) {
            console.log('[SUCCESS] Google Drive upload complete');
            await fetch(env.SLACK_WEBHOOK_URL, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                text: '<!channel> Drive保存完了',
                blocks: [
                  { type: 'header', text: { type: 'plain_text', text: '📁 Drive保存完了' } },
                  { type: 'section', text: { type: 'mrkdwn', text: `<!channel>\n📄 JSON: ${gasResult.jsonUrl}\n📝 SRT: ${gasResult.srtUrl}\n📂 ${gasResult.path}` }}
                ]
              })
            });
          }
        } catch (error) {
          console.error('[ERROR] Apps Script call failed:', error.message);
        }
      }
    } else {
      console.log('[INFO] Waiting for other job to complete...');
    }
    
    return new Response('OK', { status: 200 });
  } catch (error) {
    console.error('[ERROR]', error.message);
    await sendSlack(env, jobId, 'ERROR', null, error.message);
    return new Response('Error', { status: 500 });
  }
}

async function sendSlack(env, jobId, status, fileNameOrUrl, error) {
  if (!env.SLACK_WEBHOOK_URL) return;
  
  let message;
  
  if (status === 'SUCCESS') {
    message = {
      text: '<!channel> 完了',
      blocks: [
        { type: 'header', text: { type: 'plain_text', text: '✅ 完了' } },
        { type: 'section', fields: [
          { type: 'mrkdwn', text: `*ジョブID:*\n\`${jobId}\`` },
          { type: 'mrkdwn', text: `*ファイル:*\n\`${fileNameOrUrl}\`` }
        ]}
      ]
    };
  } else {
    message = {
      text: '<!channel> 失敗',
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
