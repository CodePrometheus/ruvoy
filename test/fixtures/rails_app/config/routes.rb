# frozen_string_literal: true

Rails.application.routes.draw do
  resources :widgets

  match "echo", to: "probes#echo", via: %i[get post put]
  get "form", to: "probes#form"
  post "form", to: "probes#submit"
  post "session", to: "probes#write_session"
  get "session", to: "probes#read_session"
  get "old", to: "probes#moved"
  get "download", to: "probes#download"
  post "upload", to: "probes#upload"
  get "boom", to: "probes#boom"
  get "stream", to: "streams#show"
  get "pid", to: "probes#pid"
  post "held_write", to: "probes#held_write"
  get "pool", to: "probes#pool"
end
