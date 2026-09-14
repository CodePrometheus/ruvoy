# frozen_string_literal: true

# Answers carry no timestamps, so two passes over the same seed data match.
class WidgetsController < ActionController::API
  def index
    widgets = Widget.order(:id)
    widgets = widgets.where(name: params[:names]) if params[:names]
    render json: widgets.map { summary(it) }
  end

  def show
    widget = Widget.find(params[:id])
    render json: summary(widget) if stale?(etag: [ widget.id, widget.name ], public: true)
  end

  def create
    widget = Widget.new(widget_params)
    if widget.save
      render json: summary(widget), status: :created, location: widget
    else
      render json: { errors: widget.errors }, status: :unprocessable_content
    end
  end

  def update
    widget = Widget.find(params[:id])
    if widget.update(widget_params)
      render json: summary(widget)
    else
      render json: { errors: widget.errors }, status: :unprocessable_content
    end
  end

  def destroy
    Widget.find(params[:id]).destroy!
    head :no_content
  end

  private

  def widget_params
    params.expect(widget: [ :name ])
  end

  def summary(widget)
    widget.slice(:id, :name)
  end
end
