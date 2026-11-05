//! Small host primitives for worker-only timers and WHATWG URL manipulation.
use rquickjs::{Ctx, Exception, Function, function::Async};
use serde_json::json;
use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Clone, Default)]
pub struct Timers(Rc<RefCell<HashMap<u32, CancellationToken>>>);
impl Timers {
    pub fn cancel_all(&self) {
        for (_, timer) in self.0.borrow_mut().drain() {
            timer.cancel();
        }
    }
}

pub fn install(ctx: &Ctx<'_>) -> rquickjs::Result<Timers> {
    let timers = Timers::default();
    let pending = timers.clone();
    // Register synchronously so clearTimeout can cancel before the async wait starts.
    ctx.globals().set(
        "__gateStartTimer",
        Function::new(ctx.clone(), move |ctx: Ctx<'_>, id: u32| {
            let mut timers = pending.0.borrow_mut();
            if timers.len() >= 256 {
                return Err(Exception::throw_range(
                    &ctx,
                    "at most 256 timers may be pending",
                ));
            }
            timers.insert(id, CancellationToken::new());
            Ok(())
        })?,
    )?;
    let pending = timers.clone();
    ctx.globals().set(
        "__gateWaitTimer",
        Function::new(
            ctx.clone(),
            Async(move |id: u32, ms: u32| {
                let pending = pending.clone();
                async move {
                    let token = pending.0.borrow().get(&id).cloned();
                    let Some(token) = token else {
                        return false;
                    };
                    let fired = tokio::select! {
                        biased;
                        () = token.cancelled() => false,
                        () = tokio::time::sleep(Duration::from_millis(ms.into())) => true,
                    };
                    pending.0.borrow_mut().remove(&id);
                    fired
                }
            }),
        )?,
    )?;
    let pending = timers.clone();
    ctx.globals().set(
        "__gateClearTimer",
        Function::new(ctx.clone(), move |id: u32| {
            if let Some(token) = pending.0.borrow_mut().remove(&id) {
                token.cancel();
            }
        })?,
    )?;
    ctx.globals().set(
        "__gateURL",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'_>,
                  input: String,
                  base: Option<String>,
                  field: Option<String>,
                  value: Option<String>| {
                url_parts(&input, base.as_deref(), field.as_deref(), value.as_deref())
                    .map_err(|()| Exception::throw_type(&ctx, "Invalid URL"))
            },
        )?,
    )?;
    ctx.globals().set(
        "__gateQueryParse",
        Function::new(ctx.clone(), |text: String| {
            serde_json::to_string(
                &url::form_urlencoded::parse(text.as_bytes())
                    .into_owned()
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        })?,
    )?;
    ctx.globals().set(
        "__gateQueryEncode",
        Function::new(ctx.clone(), |ctx: Ctx<'_>, text: String| {
            let pairs: Vec<(String, String)> = serde_json::from_str(&text)
                .map_err(|_| Exception::throw_type(&ctx, "Invalid URLSearchParams"))?;
            Ok::<_, rquickjs::Error>(
                url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(pairs)
                    .finish(),
            )
        })?,
    )?;
    ctx.eval::<(), _>(include_str!("web.js"))?;
    Ok(timers)
}

fn url_parts(
    input: &str,
    base: Option<&str>,
    field: Option<&str>,
    value: Option<&str>,
) -> Result<String, ()> {
    let base = base.map(Url::parse).transpose().map_err(|_| ())?;
    let mut url = Url::options()
        .base_url(base.as_ref())
        .parse(input)
        .map_err(|_| ())?;
    if let Some(field) = field {
        let value = value.unwrap_or_default();
        // URL property setters ignore invalid mutations; href itself throws.
        match field {
            "href" => url = Url::parse(value).map_err(|_| ())?,
            "protocol" => {
                let _ = url::quirks::set_protocol(&mut url, value);
            }
            "username" => {
                let _ = url::quirks::set_username(&mut url, value);
            }
            "password" => {
                let _ = url::quirks::set_password(&mut url, value);
            }
            "host" => {
                let _ = url::quirks::set_host(&mut url, value);
            }
            "hostname" => {
                let _ = url::quirks::set_hostname(&mut url, value);
            }
            "port" => {
                let _ = url::quirks::set_port(&mut url, value);
            }
            "pathname" => url::quirks::set_pathname(&mut url, value),
            "search" => url::quirks::set_search(&mut url, value),
            "hash" => url::quirks::set_hash(&mut url, value),
            _ => return Err(()),
        }
    }
    Ok(json!({
        "href":url.as_str(), "origin":url.origin().ascii_serialization(),
        "protocol":format!("{}:",url.scheme()), "username":url.username(), "password":url.password().unwrap_or(""),
        "host":url::quirks::host(&url), "hostname":url::quirks::hostname(&url), "port":url::quirks::port(&url),
        "pathname":url::quirks::pathname(&url), "search":url::quirks::search(&url), "hash":url::quirks::hash(&url),
    }).to_string())
}
