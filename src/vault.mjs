import {mkdir,writeFile,readFile,readdir,rename} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import path from 'node:path';

// 可靠保存库：每份资料按 资料id/版本 隔离保存，清单记录字节数与校验和。
// 重新准备产生新版本时，旧版本字节进入 quarantine，绝不把另一版资料接到旧进度后面。
export class Vault {
  constructor(root){
    this.root=root;
    this.itemsDir=path.join(root,'items');
    this.quarantineDir=path.join(root,'quarantine');
  }
  async init(){await mkdir(this.itemsDir,{recursive:true});await mkdir(this.quarantineDir,{recursive:true});}
  #itemDir(id){return path.join(this.itemsDir,id);}
  manifestPath(id,version){return path.join(this.#itemDir(id),`v${version}.manifest.json`);}
  blobPath(id,version){return path.join(this.#itemDir(id),`v${version}.blob`);}

  async hasVersion(id,version){
    try{await readFile(this.manifestPath(id,version),'utf8');return true;}
    catch{return false;}
  }

  async getManifest(id,version){
    try{return JSON.parse(await readFile(this.manifestPath(id,version),'utf8'));}
    catch{return null;}
  }

  // 原子保存：先写临时文件再 rename，清单与字节文件成对提交，崩溃也不会留下“看似完整”的半成品
  async save({id,version,content,savedAt,jobId}){
    const dir=this.#itemDir(id);
    await mkdir(dir,{recursive:true});
    const bytes=Buffer.from(content);
    const checksum=createHash('sha256').update(bytes).digest('hex');
    const blobTmp=`${this.blobPath(id,version)}.tmp-${process.pid}-${savedAt}`;
    const manifestTmp=`${this.manifestPath(id,version)}.tmp-${process.pid}-${savedAt}`;
    await writeFile(blobTmp,bytes);
    const manifest={id,version,bytes:bytes.length,checksum,savedAt,jobId};
    await writeFile(manifestTmp,JSON.stringify(manifest,null,2));
    await rename(blobTmp,this.blobPath(id,version));
    await rename(manifestTmp,this.manifestPath(id,version));
    return manifest;
  }

  // 版本切换：把某资料旧版本的已存字节移入隔离区并改名，避免与新版路径混淆
  async quarantineOldVersions(id,keepVersion,reason,at){
    let moved=[];
    let entries=[];
    try{entries=await readdir(this.#itemDir(id));}catch{return moved;}
    for(const entry of entries){
      const match=/^v(\d+)\.manifest\.json$/.exec(entry);
      if(!match||Number(match[1])===keepVersion)continue;
      const version=Number(match[1]);
      const stamp=`${id}-v${version}-${at}`;
      for(const suffix of ['manifest.json','blob']){
        const from=path.join(this.#itemDir(id),`v${version}.${suffix}`);
        const to=path.join(this.quarantineDir,`${stamp}.${suffix}`);
        try{await rename(from,to);}catch{}
      }
      moved.push({id,version,reason});
    }
    return moved;
  }

  async listSaved(){
    const result=[];
    let ids=[];
    try{ids=await readdir(this.itemsDir);}catch{return result;}
    for(const id of ids){
      let entries=[];
      try{entries=await readdir(this.#itemDir(id));}catch{continue;}
      for(const entry of entries){
        const match=/^v(\d+)\.manifest\.json$/.exec(entry);
        if(!match)continue;
        const manifest=JSON.parse(await readFile(path.join(this.#itemDir(id),entry),'utf8'));
        result.push(manifest);
      }
    }
    return result;
  }
}
