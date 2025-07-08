import os
import json
import traceback
from datetime import date, timedelta
from asyncio import CancelledError
from asyncio import run

from src.application import TikTokDownloader
from src.application.main_terminal import TikTok
from src.tools import to_upload_cookie_list
from src.uploader.upload import upload_video, upload_videos


async def do_transfer(
    downloader: TikTokDownloader,
    account_uid: str,
    account_mark: str,
    video_id: str,
    video_path: str,
    dest_account_uid: str,
    dest_account_mark: str,
):
    # 上传视频到指定的抖音账户
    downloader.console.print(
        f"开始上传视频 {video_id} 到账户 {dest_account_mark},{dest_account_uid}"
    )
    # 判断视频是否已上传过
    if await downloader.database.has_upload_data(
        dest_account_uid,
        video_id,
    ):
        # 视频已上传过，无需重复上传
        downloader.console.print(
            f"视频 {video_id} 已上传过到账户 {dest_account_mark}，无需重复上传"
        )
        return False

    # todo: 从配置中读取dest_account_uid对应的cookie
    with open("tiktok.com_raw_cookie.json", "r", encoding="utf-8") as f:
        cookies_list = json.load(f)

        basename = os.path.basename(video_path)
        filename, ext = os.path.splitext(basename)
        result = upload_video(
            video_path,
            description=filename.split("-")[-1],
            cookies_list=cookies_list,
        )
        if len(result) == 0:
            # 上传成功
            await downloader.database.write_upload_data(
                dest_account_uid, dest_account_mark, video_id, account_uid, account_mark
            )
            return True
        return False


async def transfer():
    async with TikTokDownloader() as downloader:
        try:
            await downloader.init()

            tk_instance = TikTok(
                downloader.parameter,
                downloader.database,
            )

            # 下载更新抖音账户的视频
            await tk_instance.account_detail_batch()

            # 下载更新tiktok账户的视频
            # await tk_instance.account_detail_batch_tiktok()

            transfer_configs = downloader.parameter.video_transfer
            for transfer_config in transfer_configs:
                if transfer_config.enable:
                    earliest = transfer_config.earliest
                    recorders = await downloader.recorder.database.read_download_data(
                        mark=transfer_config.from_account_mark,
                        publish_time=(date.today() - timedelta(days=earliest)).strftime(
                            "%Y-%m-%d"
                        ),
                    )
                    # todo: 重复上传过滤、单次上传数量限制
                    num = transfer_config.transfer_num
                    for recorder in recorders:
                        ret = await do_transfer(
                            downloader,
                            transfer_config.from_account_uid,
                            transfer_config.from_account_mark,
                            recorder["ID"],
                            recorder["PATH"],
                            transfer_config.to_account_uid,  # 目标抖音账户UID，暂时实际值为uid_tt
                            transfer_config.to_account_mark,
                        )
                        if ret:
                            num -= 1
                        if num <= 0:
                            break

        except (
            Exception,
            KeyboardInterrupt,
            CancelledError,
        ):
            print("服务异常退出！！！！")
            traceback.print_exc()
            return


if __name__ == "__main__":
    run(transfer())
